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
from googleapiclient.http import MediaIoBaseDownload


class DriveUtility:
    def __init__(self, user_name, credential_file_path, client_secret_path=None, logger=None):
        try:
            import argparse
            flags = argparse.ArgumentParser(parents=[argparser]).parse_args()
        except ImportError:
            flags = None

        OAUTH_SCOPE = 'https://www.googleapis.com/auth/drive'

        storage = multistore_file.get_credential_storage(filename=credential_file_path, client_id=user_name, user_agent=None, scope=OAUTH_SCOPE)
        credentials = storage.get()

        if credentials is None or credentials.invalid:
            if client_secret_path is None or not os.path.exists(client_secret_path):
                raise UnknownClientSecretsFlowError('Credentials unavailable. Please provide a valid client_secret_path to rerun authentication')

            # Run through the OAuth flow and retrieve credentials
            FLOW = flow_from_clientsecrets(client_secret_path, scope=OAUTH_SCOPE)
            credentials = run_flow(FLOW, storage, flags)

        # Create an httplib2.Http object and authorize it with our credentials
        http = httplib2.Http()
        http = credentials.authorize(http)

        service = build('drive', 'v3', http=http)

        self._service = service
        self._files = self._service.files()

        self._logger = logger

    def list_files(self, param=None, get_full_resource=False):
        result = []
        page_token = None
        while True:
            param = {} if param is None else param

            if page_token:
                param['pageToken'] = page_token

            if get_full_resource:
                param['fields'] = 'nextPageToken, files'
                files = self._files.list(**param).execute()
            else:
                files = self._files.list(**param).execute()

            for file_resource in files['files']:
                result.append(file_resource)

            page_token = files.get('nextPageToken')

            if not page_token:
                break
        return result

    def download_file(self, file_id, write_path, page_num=None, print_details=True):
        file_metadata = self._files.get(fileId=file_id, fields='name, id, mimeType, modifiedTime, size').execute()

        file_title = file_metadata['name']
        modified_date = datetime.strptime(str(file_metadata['modifiedTime']), '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=utc).astimezone(timezone('Asia/Singapore')).replace(tzinfo=None)

        if file_metadata['mimeType'] == 'application/vnd.google-apps.spreadsheet':
            assert page_num is not None

            download_url = 'https://docs.google.com/spreadsheets/d/%s/export?format=csv&gid=%i' % (file_id, page_num)
            resp, content = self._service._http.request(download_url)

            if resp.status == 200:
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
