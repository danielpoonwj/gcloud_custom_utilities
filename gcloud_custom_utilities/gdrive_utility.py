import os
import humanize
from datetime import datetime
from pytz import timezone, utc

import httplib2
from oauth2client.tools import run_flow, argparser
from oauth2client.client import flow_from_clientsecrets, UnknownClientSecretsFlowError
from oauth2client.contrib import multistore_file
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload


class DriveUtility:
    def __init__(self, user_name, credential_file_path, client_secret_path=None, logger=None, max_retries=3):
        OAUTH_SCOPE = 'https://www.googleapis.com/auth/drive'

        storage = multistore_file.get_credential_storage(filename=credential_file_path, client_id=user_name, user_agent=None, scope=OAUTH_SCOPE)
        credentials = storage.get()

        if credentials is None or credentials.invalid:
            if client_secret_path is None or not os.path.exists(client_secret_path):
                raise UnknownClientSecretsFlowError('Credentials unavailable. Please provide a valid client_secret_path to rerun authentication')

            try:
                import argparse
                flags = argparse.ArgumentParser(parents=[argparser]).parse_args()
            except ImportError:
                flags = None

            # Run through the OAuth flow and retrieve credentials
            FLOW = flow_from_clientsecrets(client_secret_path, scope=OAUTH_SCOPE)
            credentials = run_flow(FLOW, storage, flags)

        # Create an httplib2.Http object and authorize it with our credentials
        http = httplib2.Http()
        http = credentials.authorize(http)

        service = build('drive', 'v3', http=http)

        self._service = service
        self._files = self._service.files()
        self._about = self._service.about()

        # Number of bytes to send/receive in each request.
        self._CHUNKSIZE = 2 * 1024 * 1024

        self._logger = logger
        self._max_retries = max_retries

    def get_account_info(self, fields=None):
        if fields is None:
            fields = 'appInstalled, ' \
                     'exportFormats, ' \
                     'folderColorPalette, ' \
                     'importFormats, ' \
                     'kind, ' \
                     'maxImportSizes, ' \
                     'maxUploadSize, ' \
                     'storageQuota, ' \
                     'user'

        return self._about.get(fields=fields).execute(num_retries=self._max_retries)

    def list_files(self, param=None, get_full_resource=False):
        result = []
        page_token = None
        while True:
            param = {} if param is None else param

            if page_token:
                param['pageToken'] = page_token

            if get_full_resource:
                param['fields'] = 'nextPageToken, files'
                files = self._files.list(**param).execute(num_retries=self._max_retries)
            else:
                files = self._files.list(**param).execute(num_retries=self._max_retries)

            for file_resource in files['files']:
                result.append(file_resource)

            page_token = files.get('nextPageToken')

            if not page_token:
                break
        return result

    def download_file(self, file_id, write_path, page_num=None, print_details=True, output_type=None):
        file_metadata = self._files.get(fileId=file_id, fields='name, id, mimeType, modifiedTime, size').execute(num_retries=self._max_retries)

        file_title = file_metadata['name']
        modified_date = datetime.strptime(str(file_metadata['modifiedTime']), '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=utc).astimezone(timezone('Asia/Singapore')).replace(tzinfo=None)

        return_data = None

        if file_metadata['mimeType'] == 'application/vnd.google-apps.spreadsheet':
            assert page_num is not None

            download_url = 'https://docs.google.com/spreadsheets/d/%s/export?format=csv&gid=%i' % (file_id, page_num)
            resp, content = self._service._http.request(download_url)

            if resp.status == 200:

                if output_type is not None:
                    assert output_type in ('dataframe', 'list')
                    from io import BytesIO

                    with BytesIO(content) as file_buffer:
                        if output_type == 'list':
                            import unicodecsv as csv
                            return_data = list(csv.reader(file_buffer))
                        elif output_type == 'dataframe':
                            import pandas as pd
                            return_data = pd.read_csv(file_buffer)

                else:
                    with open(write_path, 'wb') as write_file:
                        write_file.write(content)

                logging_string = '[Drive] Downloaded %s [%s]. Last Modified: %s' % (file_title, file_id, modified_date)
            else:
                raise HttpError(resp, content)

        else:
            request = self._files.get_media(fileId=file_id)

            with open(write_path, 'wb') as write_file:
                downloader = MediaIoBaseDownload(write_file, request)

                done = False
                while done is False:
                    status, done = downloader.next_chunk()

            file_size = humanize.naturalsize(int(file_metadata['size']))
            logging_string = '[Drive] Downloaded %s [%s] (%s). Last Modified: %s' % (file_title, file_id, file_size, modified_date)

        if print_details:
            print '\t' + logging_string

        if self._logger is not None:
            self._logger.info(logging_string)

        return return_data

    def upload_file(self, read_path, description=None, parent_id=None, overwrite_existing=True, print_details=True):
        file_name = os.path.basename(read_path)

        fields = 'id, name, size, modifiedTime'

        # check for existing file
        q = 'name="%s"' % file_name

        request_body = {
            'name': file_name
        }

        if description is not None:
            request_body['description'] = description

        if parent_id is not None:
            assert isinstance(parent_id, str)
            request_body['parents'] = parent_id

            q = '%s and "%s" in parents' % (q, parent_id)

        existing_files = self.list_files({'q': q})

        media = MediaFileUpload(read_path, chunksize=self._CHUNKSIZE, resumable=True)

        if len(existing_files) == 0:
            response = self._files.create(
                media_body=media,
                body=request_body,
                fields=fields
            ).execute(num_retries=self._max_retries)

            modified_date = datetime.strptime(str(response['modifiedTime']), '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=utc).astimezone(timezone('Asia/Singapore')).replace(tzinfo=None)

            logging_string = '[Drive] Uploaded (Created) %s [%s] (%s). Last Modified: %s' % (
                response['name'],
                response['id'],
                humanize.naturalsize(int(response['size'])),
                modified_date
            )

        elif len(existing_files) == 1 and overwrite_existing:
            if 'parents' in request_body:
                del request_body['parents']

            response = self._files.update(
                fileId=existing_files[0]['id'],
                media_body=media,
                body=request_body,
                fields=fields
            ).execute(num_retries=self._max_retries)

            modified_date = datetime.strptime(str(response['modifiedTime']), '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=utc).astimezone(timezone('Asia/Singapore')).replace(tzinfo=None)

            logging_string = '[Drive] Uploaded (Replaced) %s [%s] (%s). Last Modified: %s' % (
                response['name'],
                response['id'],
                humanize.naturalsize(int(response['size'])),
                modified_date
            )

        else:
            raise ValueError('Multiple existing files named %s found in folder' % file_name)

        if print_details:
            print '\t' + logging_string

        if self._logger is not None:
            self._logger.info(logging_string)

        return response
