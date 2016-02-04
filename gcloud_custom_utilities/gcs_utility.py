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

    def __init__(self):
        credentials = GoogleCredentials.get_application_default()
        service = build('storage', 'v1', credentials=credentials)

        self.service = service
        self.buckets = self.service.buckets()
        self.objects = self.service.objects()

        # Retry transport and file IO errors.
        self.RETRYABLE_ERRORS = (HttpLib2Error, IOError)

        # Number of times to retry failed downloads.
        self.NUM_RETRIES = 5

        # Number of bytes to send/receive in each request.
        self.CHUNKSIZE = 2 * 1024 * 1024

        # Mimetype to use if one can't be guessed from the file extension.
        self.DEFAULT_MIMETYPE = 'application/octet-stream'

    def list_buckets(self, project, max_results=None):
        buckets_list = []

        response = self.buckets.list(
            project=project,
            maxResults=max_results
        ).execute()

        buckets_list += response['items']

        while 'nextPageToken' in response and max_results is None:
            page_token = None
            if 'nextPageToken' in response:
                page_token = response['nextPageToken']

            response = self.buckets.list(
                project=project,
                pageToken=page_token
            ).execute()

            if 'items' in response:
                buckets_list += response['items']

        return buckets_list

    def list_objects(self, bucket, prefix=None, max_results=None):
        objects_list = []

        response = self.objects.list(
            bucket=bucket,
            prefix=prefix,
            maxResults=max_results
        ).execute()

        if 'items' in response:
            objects_list += response['items']

            while 'nextPageToken' in response and max_results is None:
                page_token = None
                if 'nextPageToken' in response:
                    page_token = response['nextPageToken']

                response = self.objects.list(
                    bucket=bucket,
                    prefix=prefix,
                    pageToken=page_token
                ).execute()

                if 'items' in response:
                    objects_list += response['items']
        
        return objects_list

    def __parse_object_name(self, object, subfolders=None):

        if subfolders:
            if isinstance(subfolders, list):
                object_updated = quote('%s/%s' % ('/'.join(subfolders), object))
            else:
                raise TypeError('subfolders should be a list of folder directories')
        else:
            object_updated = object

        return object_updated

    def get_object_metadata(self, bucket, object, subfolders=None):

        response = self.objects.get(
            bucket=bucket,
            object=self.__parse_object_name(object, subfolders)
        ).execute()

        return response

    def __handle_progressless_iter(self, error, progressless_iters):
        if progressless_iters > self.NUM_RETRIES:
            print 'Failed to make progress for too many consecutive iterations.'
            raise error

        sleeptime = random.random() * (2**progressless_iters)
        print ('Caught exception (%s). Sleeping for %s seconds before retry #%d.'
                % (str(error), sleeptime, progressless_iters))
        time.sleep(sleeptime)

    def download_object(self, bucket, object, write_path, subfolders=None):
        write_file = file(write_path, 'wb')

        request = self.objects.get_media(
            bucket=bucket,
            object=self.__parse_object_name(object, subfolders)
        )

        media = MediaIoBaseDownload(write_file, request, chunksize=self.CHUNKSIZE)

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
            except self.RETRYABLE_ERRORS, err:
                error = err

            if error:
                progressless_iters += 1
                self.__handle_progressless_iter(error, progressless_iters)
            else:
                progressless_iters = 0

        file_size = humanize.naturalsize(int(self.get_object_metadata(bucket, object, subfolders)['size']))

        print 'Downloaded %s:%s (%s)' % (bucket, object, file_size)

    def upload_object(self, bucket, object, read_path, subfolders=None):
        media = MediaFileUpload(read_path, chunksize=self.CHUNKSIZE, resumable=True)

        if not media.mimetype():
            media = MediaFileUpload(read_path, self.DEFAULT_MIMETYPE, resumable=True)
        
        request = self.objects.insert(
            bucket=bucket, 
            name=self.__parse_object_name(object, subfolders),
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
            except self.RETRYABLE_ERRORS, err:
                error = err

            if error:
                progressless_iters += 1
                self.__handle_progressless_iter(error, progressless_iters)
            else:
                progressless_iters = 0

        file_size = humanize.naturalsize(int(response['size']))

        print 'Uploaded to %s:%s (%s)' % (bucket, object, file_size)
