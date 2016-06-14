import os

import httplib2
from oauth2client.tools import run_flow, argparser
from oauth2client.client import flow_from_clientsecrets, UnknownClientSecretsFlowError
from oauth2client.contrib import multistore_file
from googleapiclient.discovery import build


class GtmUtility:
    def __init__(self, user_name, credential_file_path, client_secret_path=None, logger=None):
        OAUTH_SCOPE = 'https://www.googleapis.com/auth/tagmanager.readonly'

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

        service = build('tagmanager', 'v1', http=http)

        self._service = service
        self._accounts = self._service.accounts()

        self._permissions = self._accounts.permissions()
        self._containers = self._accounts.containers()

        self._versions = self._containers.versions()
        self._variables = self._containers.variables()
        self._tags = self._containers.tags()
        self._triggers = self._containers.triggers()

        self._logger = logger

    def list_accounts(self):
        return self._accounts.list().execute()

    def list_containers(self, account_id):
        return self._containers.list(
            accountId=account_id
        ).execute()

    def list_tags(self, account_id, container_id):
        return self._tags.list(
            accountId=account_id,
            containerId=container_id
        ).execute()

    def list_triggers(self, account_id, container_id):
        return self._triggers.list(
            accountId=account_id,
            containerId=container_id
        ).execute()

    def list_variables(self, account_id, container_id):
        return self._variables.list(
            accountId=account_id,
            containerId=container_id
        ).execute()
