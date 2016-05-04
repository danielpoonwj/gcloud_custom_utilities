import humanize
from time import sleep
from datetime import datetime
from pytz import UTC
from urllib2 import quote
from httplib2 import HttpLib2Error
import random

from oauth2client.client import GoogleCredentials, ApplicationDefaultCredentialsError, flow_from_clientsecrets, UnknownClientSecretsFlowError
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload


class GcsUtility:
    def __init__(self, logger=None, authentication_type='Default Credentials', credential_file_path=None, user_name=None, client_secret_path=None):

        if authentication_type == 'Default Credentials':
            # try building from application default
            try:
                credentials = GoogleCredentials.get_application_default()
                service = build('storage', 'v1', credentials=credentials)
            except ApplicationDefaultCredentialsError as e:
                print 'Application Default Credentials unavailable. To set up Default Credentials, download gcloud from https://cloud.google.com/sdk/gcloud/ and authenticate through gcloud auth login'
                raise e

        elif authentication_type == 'Stored Credentials':
            import os
            import httplib2
            from oauth2client.contrib import multistore_file
            from oauth2client.tools import run_flow, argparser

            try:
                import argparse
                flags = argparse.ArgumentParser(parents=[argparser]).parse_args()
            except ImportError:
                flags = None

            OAUTH_SCOPE = 'https://www.googleapis.com/auth/cloud-platform'

            assert user_name is not None and credential_file_path is not None
            storage = multistore_file.get_credential_storage(filename=credential_file_path, client_id=user_name, user_agent=None, scope=OAUTH_SCOPE)
            credentials = storage.get()

            if credentials is None or credentials.invalid:
                if client_secret_path is None or not os.path.exists(client_secret_path):
                    raise UnknownClientSecretsFlowError('Credentials unavailable. Please provide a valid client_secret_path to rerun authentication')

                FLOW = flow_from_clientsecrets(client_secret_path, scope=OAUTH_SCOPE)
                credentials = run_flow(FLOW, storage, flags)

            # Create an httplib2.Http object and authorize it with your credentials
            http = httplib2.Http()
            http = credentials.authorize(http)

            service = build('storage', 'v1', http=http)
        else:
            raise TypeError('Authentication types available are "Default Credentials" and "Stored Credentials"')

        self._service = service
        self._buckets = self._service.buckets()
        self._objects = self._service.objects()

        # Retry transport and file IO errors.
        self._RETRYABLE_ERRORS = (HttpLib2Error, IOError)

        # Number of times to retry failed downloads.
        self._NUM_RETRIES = 5

        # Number of bytes to send/receive in each request.
        self._CHUNKSIZE = 2 * 1024 * 1024

        # Mimetype to use if one can't be guessed from the file extension.
        self._DEFAULT_MIMETYPE = 'application/octet-stream'

        self._logger = logger

    def list_buckets(self, project_name, max_results=None):
        buckets_list = []
        buckets_count = 0

        response = self._buckets.list(
            project=project_name,
            maxResults=max_results
        ).execute()

        if 'items' in response:
            buckets_list += response['items']
            buckets_count += len(response['items'])
        else:
            return buckets_list

        if buckets_count > max_results:
            buckets_list = buckets_list[:max_results]
            return buckets_list

        while 'nextPageToken' in response:
            page_token = None
            if 'nextPageToken' in response:
                page_token = response['nextPageToken']

            response = self._buckets.list(
                project=project_name,
                pageToken=page_token
            ).execute()

            if 'items' in response:
                buckets_list += response['items']
                buckets_count += len(response['items'])

            if buckets_count > max_results:
                buckets_list = buckets_list[:max_results]
                break

        return buckets_list

    def list_objects(self, bucket_name, search_prefix=None, max_results=None):
        objects_list = []
        objects_count = 0

        response = self._objects.list(
            bucket=bucket_name,
            prefix=search_prefix,
            maxResults=max_results
        ).execute()

        if 'items' in response:
            objects_list += response['items']
            objects_count += len(response['items'])
        else:
            return objects_list

        if objects_count > max_results:
            objects_list = objects_list[:max_results]
            return objects_list

        while 'nextPageToken' in response:
            page_token = None
            if 'nextPageToken' in response:
                page_token = response['nextPageToken']

            response = self._objects.list(
                bucket=bucket_name,
                prefix=search_prefix,
                pageToken=page_token
            ).execute()

            if 'items' in response:
                objects_list += response['items']
                objects_count += len(response['items'])

            if objects_count > max_results:
                objects_list = objects_list[:max_results]
                break

        return objects_list

    def _parse_object_name(self, object_name, subfolders=None):
        object_updated = object_name

        if subfolders:
            assert isinstance(subfolders, list), 'subfolders should be a list of folder directories'
            object_updated = quote('%s/%s' % ('/'.join(subfolders), object_name))

        return object_updated

    def get_object_metadata(self, bucket_name, object_name, subfolders=None):
        response = self._objects.get(
            bucket=bucket_name,
            object=self._parse_object_name(object_name, subfolders)
        ).execute()

        return response

    def _handle_progressless_iter(self, error, progressless_iters):
        if progressless_iters > self._NUM_RETRIES:
            print 'Failed to make progress for too many consecutive iterations.'
            raise error

        sleeptime = random.random() * (2**progressless_iters)
        print ('Caught exception (%s). Sleeping for %s seconds before retry #%d.'
                % (str(error), sleeptime, progressless_iters))
        sleep(sleeptime)

    def download_object(self, bucket_name, object_name, write_path, subfolders=None, print_details=True):
        write_file = file(write_path, 'wb')

        request = self._objects.get_media(
            bucket=bucket_name,
            object=self._parse_object_name(object_name, subfolders)
        )

        media = MediaIoBaseDownload(write_file, request, chunksize=self._CHUNKSIZE)

        progressless_iters = 0
        done = False

        while not done:
            error = None
            try:
                progress, done = media.next_chunk()
            except HttpError, err:
                error = err
                if err.resp.status < 500:
                    raise
            except self._RETRYABLE_ERRORS, err:
                error = err

            if error:
                progressless_iters += 1
                self._handle_progressless_iter(error, progressless_iters)
            else:
                progressless_iters = 0

        meta_data = self.get_object_metadata(bucket_name, object_name, subfolders)
        file_size = humanize.naturalsize(int(meta_data['size']))

        logging_string = '[GCS] Downloaded gs://%s/%s (%s)' % (meta_data['bucket'], meta_data['name'], file_size)

        if print_details:
            print '\t' + logging_string

        if self._logger is not None:
            self._logger.info(logging_string)

    def upload_object(self, bucket_name, object_name, read_path, subfolders=None, print_details=True):
        process_start_time = datetime.now(UTC)

        media = MediaFileUpload(read_path, chunksize=self._CHUNKSIZE, resumable=True)

        if not media.mimetype():
            media = MediaFileUpload(read_path, self._DEFAULT_MIMETYPE, resumable=True)
        
        request = self._objects.insert(
            bucket=bucket_name,
            name=self._parse_object_name(object_name, subfolders),
            media_body=media
        )

        progressless_iters = 0
        response = None
        while response is None:
            error = None
            try:
                progress, response = request.next_chunk()
            except HttpError, err:
                error = err
                if err.resp.status < 500:
                    raise
            except self._RETRYABLE_ERRORS, err:
                error = err

            if error:
                progressless_iters += 1
                self._handle_progressless_iter(error, progressless_iters)
            else:
                progressless_iters = 0

        file_size = humanize.naturalsize(int(response['size']))

        # for logging
        m, s = divmod(
                (
                    UTC.localize(datetime.strptime(response['updated'], '%Y-%m-%dT%H:%M:%S.%fZ')) -
                    process_start_time
                ).seconds, 60)

        time_taken = '%02d Minutes %02d Seconds' % (m, s)

        logging_string = '[GCS] Uploaded to gs://%s/%s [%s] (%s)' % (
            response['bucket'],
            response['name'],
            file_size,
            time_taken
        )

        if print_details:
            print '\t' + logging_string

        if self._logger is not None:
            self._logger.info(logging_string)

        return response

    def delete_object(self, bucket_name, object_name, subfolders=None, print_details=True):
        response = None
        while response is None:
            response = self._objects.delete(
                bucket=bucket_name,
                object=self._parse_object_name(object_name, subfolders)
            ).execute()

        logging_string = '[GCS] Deleted gs://%s/%s' % (
            bucket_name,
            self._parse_object_name(object_name, subfolders)
        )

        if print_details:
            print '\t' + logging_string

        if self._logger is not None:
            self._logger.info(logging_string)

        return response
