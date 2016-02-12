import humanize
import time
from urllib2 import quote
from httplib2 import HttpLib2Error
import random

from oauth2client.client import GoogleCredentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload


class GcsUtility:

    def __init__(self, logger=None):
        credentials = GoogleCredentials.get_application_default()
        service = build('storage', 'v1', credentials=credentials)

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
        time.sleep(sleeptime)

    def download_object(self, bucket_name, object_name, write_path, subfolders=None, print_results=True):
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

        file_size = humanize.naturalsize(int(self.get_object_metadata(bucket_name, object_name, subfolders)['size']))

        logging_string = 'Downloaded %s:%s (%s)' % (bucket_name, object_name, file_size)

        if print_results:
            print logging_string

        if self._logger is not None:
            self._logger.info(logging_string)

    def upload_object(self, bucket_name, object_name, read_path, subfolders=None, print_results=True):
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

        logging_string = 'Uploaded to %s:%s (%s)' % (bucket_name, object_name, file_size)

        if print_results:
            print logging_string

        if self._logger is not None:
            self._logger.info(logging_string)
