import humanize
from time import sleep
from datetime import datetime
from urllib2 import quote
from httplib2 import HttpLib2Error
import random

from oauth2client.client import GoogleCredentials, ApplicationDefaultCredentialsError, flow_from_clientsecrets
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload


class GcsUtility:
    def __init__(self, logger=None):
        try:
            credentials = GoogleCredentials.get_application_default()
            service = build('storage', 'v1', credentials=credentials)

        except ApplicationDefaultCredentialsError:
            import os
            import sys
            import httplib2
            from oauth2client.file import Storage
            from oauth2client.tools import run_flow, argparser

            try:
                import argparse
                flags = argparse.ArgumentParser(parents=[argparser]).parse_args()
            except ImportError:
                flags = None

            OAUTH_SCOPE = 'https://www.googleapis.com/auth/cloud-platform'

            print 'Application Default Credentials unavailable.'
            print 'To set up Default Credentials, download gcloud from https://cloud.google.com/sdk/gcloud/ and authenticate through gcloud auth login'

            to_continue = None

            while to_continue not in ('y', 'n'):
                to_continue = raw_input('Alternatively, authenticate through Client Secret? [y/n]: ').lower()

            if to_continue == 'n':
                sys.exit(0)

            print 'Input client secret path. For more detailed instructions, press enter.'
            CLIENT_SECRET = raw_input('Client Secret Path: ').strip()

            if CLIENT_SECRET is None or not os.path.exists(CLIENT_SECRET):
                print 'Instructions for generating Client Secret file:'
                print '1. Go to https://console.developers.google.com/'
                print '2. Under the Projects dropdown menu, click create a project. This will be a project specific to your login account'
                print '3. Once the new project is created, select that project, and navigate to API Manager'
                print '4. Under the API Manager submenu, click on Credentials and click Create credentials. Select OAuth client ID, with the Application type as Other.'
                print '5. After it has been successfully created, you will have the option of downloading it as json.'
                sys.exit(0)

            print 'Input credentials filepath. If file does not currently exist, one will be created for you.\n'
            CREDS_FILE = raw_input('Credentials Path: ').strip()

            storage = Storage(CREDS_FILE)
            credentials = storage.get()

            FLOW = flow_from_clientsecrets(CLIENT_SECRET, scope=OAUTH_SCOPE)

            if credentials is None or credentials.invalid:
                # Run through the OAuth flow and retrieve credentials
                credentials = run_flow(FLOW, storage, flags)

            # Create an httplib2.Http object and authorize it with our credentials
            http = httplib2.Http()
            http = credentials.authorize(http)

            service = build('storage', 'v1', http=http)

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

        response = self._buckets.list(
            project=project_name,
            maxResults=max_results
        ).execute()

        buckets_list += response['items']

        while 'nextPageToken' in response and max_results is None:
            page_token = None
            if 'nextPageToken' in response:
                page_token = response['nextPageToken']

            response = self._buckets.list(
                project=project_name,
                pageToken=page_token
            ).execute()

            if 'items' in response:
                buckets_list += response['items']

        return buckets_list

    def list_objects(self, bucket_name, search_prefix=None, max_results=None):
        objects_list = []

        response = self._objects.list(
            bucket=bucket_name,
            prefix=search_prefix,
            maxResults=max_results
        ).execute()

        if 'items' in response:
            objects_list += response['items']

            while 'nextPageToken' in response and max_results is None:
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
        process_start_time = datetime.utcnow()

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
                    datetime.strptime(response['updated'], '%Y-%m-%dT%H:%M:%S.%fZ') -
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
