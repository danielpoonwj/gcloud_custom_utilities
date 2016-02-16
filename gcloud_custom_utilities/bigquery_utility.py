import humanize
import uuid
import pandas as pd
import time

from oauth2client.client import GoogleCredentials, ApplicationDefaultCredentialsError, flow_from_clientsecrets
from googleapiclient.discovery import build
from googleapiclient.errors import Error
from googleapiclient.http import MediaInMemoryUpload
pd.set_option('expand_frame_repr', False)


def read_string_from_file(read_path):
    with open(read_path, 'rb') as read_file:
        read_string = read_file.read()
    return read_string


class BigqueryUtility:
    def __init__(self, logger=None):
        try:
            # try building from application default
            credentials = GoogleCredentials.get_application_default()
            service = build('bigquery', 'v2', credentials=credentials)

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

            OAUTH_SCOPE = 'https://www.googleapis.com/auth/bigquery'

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

            service = build('bigquery', 'v2', http=http)

        self._service = service
        self._datasets = self._service.datasets()
        self._jobs = self._service.jobs()
        self._projects = self._service.projects()
        self._tabledata = self._service.tabledata()
        self._tables = self._service.tables()

        self._logger = logger

    def list_projects(self, max_results=None):
        project_list = []
        project_count = 0

        response = self._projects.list().execute()

        project_list += response['projects']
        project_count += len(response['projects'])

        if project_count > max_results:
            project_list = project_list[:max_results]
            return project_list

        while 'nextPageToken' in response:
            page_token = None
            if 'nextPageToken' in response:
                page_token = response['nextPageToken']

            response = self._projects.list(
                pageToken=page_token
            ).execute()

            if 'projects' in response:
                project_list += response['projects']
                project_count += len(response['projects'])

            if project_count > max_results:
                project_list = project_list[:max_results]
                break

        return project_list

    def list_jobs(self, project_id, state_filter=None, show_all_users=False, max_results=None):
        job_list = []
        job_count = 0

        response = self._jobs.list(
            projectId=project_id,
            allUsers=show_all_users,
            stateFilter=state_filter
        ).execute()

        job_list += response['jobs']
        job_count += len(response['jobs'])

        if job_count > max_results:
            job_list = job_list[:max_results]
            return job_list

        while 'nextPageToken' in response:
            page_token = None
            if 'nextPageToken' in response:
                page_token = response['nextPageToken']

            response = self._jobs.list(
                projectId=project_id,
                allUsers=show_all_users,
                stateFilter=state_filter,
                pageToken=page_token
            ).execute()

            if 'jobs' in response:
                job_list += response['jobs']
                job_count += len(response['jobs'])

            if job_count > max_results:
                job_list = job_list[:max_results]
                break

        return job_list

    def list_datasets(self, project_id, show_all=False, max_results=None):
        dataset_list = []
        dataset_count = 0

        response = self._datasets.list(
            projectId=project_id,
            all=show_all
        ).execute()

        dataset_list += response['datasets']
        dataset_count += len(response['datasets'])

        if dataset_count > max_results:
            dataset_list = dataset_list[:max_results]
            return dataset_list

        while 'nextPageToken' in response:
            page_token = None
            if 'nextPageToken' in response:
                page_token = response['nextPageToken']

            response = self._datasets.list(
                projectId=project_id,
                all=show_all,
                pageToken=page_token
            ).execute()

            if 'datasets' in response:
                dataset_list += response['datasets']
                dataset_count += len(response['datasets'])

            if dataset_count > max_results:
                dataset_list = dataset_list[:max_results]
                break

        return dataset_list

    def list_tables(self, project_id, dataset_id, max_results=None):
        table_list = []
        table_count = 0

        response = self._tables.list(
            projectId=project_id,
            datasetId=dataset_id
        ).execute()

        table_list += response['tables']
        table_count += len(response['tables'])

        if table_count > max_results:
            table_list = table_list[:max_results]
            return table_list

        while 'nextPageToken' in response:
            page_token = None
            if 'nextPageToken' in response:
                page_token = response['nextPageToken']

            response = self._tables.list(
                projectId=project_id,
                datasetId=dataset_id,
                pageToken=page_token
            ).execute()

            if 'tables' in response:
                table_list += response['tables']
                table_count += len(response['tables'])

            if table_count > max_results:
                table_list = table_list[:max_results]
                break

        return table_list

    def get_job(self, project_id, job_id):
        return self._jobs.get(
            projectId=project_id,
            jobId=job_id
        ).execute()

    def get_table_info(self, project_id, dataset_id, table_id):
        return self._tables.get(
            projectId=project_id,
            datasetId=dataset_id,
            tableId=table_id
        ).execute()

    def get_sharded_date_range(self, project_id, dataset_id, print_results=True):
        tableList = self.list_tables(project_id, dataset_id)

        tableDict = {}
        returnDict = {}

        for table in tableList:
            tableName = table['tableReference']['tableId']
            groupName = tableName[:-8]

            try:
                tableDate = pd.datetime.strptime(tableName[-8:], '%Y%m%d')

                if groupName not in tableDict:
                    tableDict[groupName] = []

                tableDict[groupName].append(tableDate)
            except ValueError:
                pass

        if len(tableDict) > 0:
            for key in tableDict.keys():
                tableDateList = tableDict[key]
                min_date = min(tableDateList)
                max_date = max(tableDateList)

                dateRange = list(pd.date_range(min_date, max_date))
                missing_dates = [x.to_datetime() for x in dateRange if x not in tableDateList]

                returnDict[key] = {
                    'min_date': min_date,
                    'max_date': max_date,
                    'missing_dates': missing_dates
                }

            if print_results:
                for key in returnDict.keys():
                    print 'Group Name: %s' % key
                    print 'Min Date: %s' % returnDict[key]['min_date'].strftime('%Y-%m-%d')
                    print 'Max Date: %s' % returnDict[key]['max_date'].strftime('%Y-%m-%d')
                    print 'Missing Dates: %s' % ', '.join([x.strftime('%Y-%m-%d') for x in returnDict[key]['missing_dates']])
                    print
        else:
            raise ValueError('No sharded tables found')

        return returnDict

    def _get_query_schema(self, response):

        response = self._jobs.getQueryResults(
                projectId=response['jobReference']['projectId'],
                jobId=response['jobReference']['jobId'],
                maxResults=0
            ).execute()

        return response['schema']['fields']

    def delete_table(self, project_id, dataset_id, table_id, print_results=True):
        self._tables.delete(
            projectId=project_id,
            datasetId=dataset_id,
            tableId=table_id
        ).execute()

        logging_string = 'Deleted %s:%s:%s' % (project_id, dataset_id, table_id)

        if print_results:
            print '\t%s' % logging_string

        if self._logger is not None:
            self._logger.info(logging_string)

    def query(self, project_id, query, async=False, async_data=None, udfInlineCode=None, return_type='list', print_details=True):
        """Submit a query to bigquery. Users can choose whether to submit an
        asynchronous or synchronous query (default)."""
        if async:
            # projectId, datasetId and tableId must be filled for async queries
            write_project_id = async_data['projectId']
            write_dataset_id = async_data['datasetId']
            write_table_id = async_data['tableId']

            return self._async_query(project_id, query, write_project_id, write_dataset_id, write_table_id, udfInlineCode, return_type, print_details)
        else:
            if udfInlineCode is not None:
                print 'WARNING: UDF is not enabled for sync queries, please use async if UDF is required'

                if self._logger is not None:
                    self._logger.warn('UDF is not enabled for sync queries, please use async if UDF is required')

            return self._sync_query(project_id, query, return_type, print_details)

    def _sync_query(self, project_id, query, return_type, print_details):
        request_body = {
            'query': query,
            'timeoutMs': 0
        }

        response = self._jobs.query(
            projectId=project_id,
            body=request_body
        ).execute()

        return self._iterate_job_results(response, return_type, print_details)

    def _async_query(self, project_id, query, write_project_id, write_dataset_id, write_table_id, udfInlineCode, return_type, print_details):

        request_body = {
            'jobReference': {
                'projectId': project_id,
                'job_id': str(uuid.uuid4())
            },
            'configuration': {
                'query': {

                    'userDefinedFunctionResources': [
                        None if udfInlineCode is None else {'inlineCode': udfInlineCode}
                    ],

                    'query': query,
                    'allowLargeResults': 'true',
                    'destinationTable': {
                        'projectId': write_project_id,
                        'datasetId': write_dataset_id,
                        'tableId': write_table_id,
                    },
                    'writeDisposition': 'WRITE_TRUNCATE'
                }
            }
        }

        response = self._jobs.insert(
            projectId=project_id,
            body=request_body
        ).execute()

        return self._iterate_job_results(response, return_type, print_details)

    def _iterate_job_results(self, response, returnType, print_details):
        start_time = time.time()

        self.poll_job_status(response)

        returnList = []

        job_reference = response['jobReference']
        isComplete = False

        while not isComplete or 'pageToken' in response:
            page_token = None
            if 'pageToken' in response:
                page_token = response['pageToken']

            response = self._jobs.getQueryResults(
                projectId=job_reference['projectId'],
                jobId=job_reference['jobId'],
                timeoutMs=0,
                pageToken=page_token
            ).execute()

            isComplete = response['jobComplete']

            if 'rows' in response:
                if page_token is None:
                    returnList.append([item['name'] for item in response['schema']['fields']])

                for row in response['rows']:
                    returnList.append([item['v'] for item in row['f']])

            time.sleep(1)

        m, s = divmod((time.time() - start_time), 60)
        timeTaken = '%02d Minutes %02d Seconds' % (m, s)

        logging_string = 'Data retrieved with %d rows and %s processed (%s)' % (
                int(response['totalRows']),
                humanize.naturalsize(int(response['totalBytesProcessed'])),
                timeTaken
            )

        if print_details:
            print '\t%s' % logging_string

        if self._logger is not None:
            self._logger.info(logging_string)

        if returnType == 'list':
            return returnList
        elif returnType == 'dataframe':
            querySchema = self._get_query_schema(response)

            def _convert_timestamp(input_value):
                try:
                    return pd.datetime.utcfromtimestamp(float(input_value))
                except TypeError:
                    return pd.np.NaN

            dtypeConversion = {
                'TIMESTAMP': lambda x: _convert_timestamp(x)
            }

            if len(returnList) > 0:
                resultHeader = returnList.pop(0)
                returnDF = pd.DataFrame(returnList, columns=resultHeader)

                for fieldDict in querySchema:
                    if fieldDict['type'] in dtypeConversion.keys():
                        returnDF[fieldDict['name']] = returnDF[fieldDict['name']].map(dtypeConversion[fieldDict['type']])

                return returnDF
            else:
                return None

        else:
            raise TypeError('Data can only be exported as list or dataframe')

    def poll_job_status(self, response):
        try:
            status_state = response['status']['state']
        except KeyError:
            status_state = None

        while not status_state == 'DONE':
            response = self._jobs.get(
                jobId=response['jobReference']['jobId'],
                projectId=response['jobReference']['projectId']
            ).execute()

            status_state = response['status']['state']

        if 'errorResult' in response['status']:
            err_msg = '%s: %s' % (response['status']['errorResult']['reason'], response['status']['errorResult']['message'])
            raise Error(err_msg)

        return response

    def write_table(self,
                    project_id,
                    query,
                    writeData,
                    writeDisposition='WRITE_TRUNCATE',
                    udfInlineCode=None,
                    print_details=True,
                    wait_finish=True):
        start_time = time.time()

        # projectId, datasetId and tableId must be filled when writing to table
        write_project_id = writeData['projectId']
        write_dataset_id = writeData['datasetId']
        write_table_id = writeData['tableId']

        request_body = {
            'jobReference': {
                'projectId': project_id,
                'job_id': str(uuid.uuid4())
            },
            'configuration': {
                'query': {

                    'userDefinedFunctionResources': [
                        None if udfInlineCode is None else {'inlineCode': udfInlineCode}
                    ],

                    'query': query,
                    'allowLargeResults': 'true',
                    'destinationTable': {
                        'projectId': write_project_id,
                        'datasetId': write_dataset_id,
                        'tableId': write_table_id,
                    },
                    'writeDisposition': writeDisposition
                }
            }
        }

        response = self._jobs.insert(
            projectId=project_id,
            body=request_body
        ).execute()

        if wait_finish:
            self.poll_job_status(response)

            response = self._jobs.getQueryResults(
                projectId=project_id,
                jobId=response['jobReference']['jobId']
            ).execute()

            m, s = divmod((time.time() - start_time), 60)
            timeTaken = '%02d Minutes %02d Seconds' % (m, s)

            logging_string = '\tQuery %s to %s with %d rows and %s processed (%s)' % (
                    'appended' if writeDisposition == 'WRITE_APPEND' else 'written',
                    '%s:%s:%s' % (write_project_id, write_dataset_id, write_table_id),
                    int(response['totalRows']),
                    humanize.naturalsize(int(response['totalBytesProcessed'])),
                    timeTaken
                )
            if print_details:
                print '\t%s' % logging_string

            if self._logger is not None:
                self._logger.info(logging_string)
        else:
            return response

    def load_from_gcs(self,
                      writeData,
                      writeDisposition='WRITE_TRUNCATE',
                      skipHeader=True,
                      print_details=True,
                      wait_finish=True):

        start_time = time.time()

        # projectId, datasetId, tableId, schemaFields, sourceUri must be filled for load jobs
        write_project_id = writeData['projectId']
        write_dataset_id = writeData['datasetId']
        write_table_id = writeData['tableId']
        schema_fields = writeData['schemaFields']
        source_uri = writeData['sourceUri']

        request_body = {
            'jobReference': {
                'projectId': write_project_id,
                'job_id': str(uuid.uuid4())
            },

            'configuration': {
                'load': {
                    'destinationTable': {
                        'projectId': write_project_id,
                        'datasetId': write_dataset_id,
                        'tableId': write_table_id
                    },
                    'writeDisposition': writeDisposition,
                    'skipLeadingRows': 1 if skipHeader else 0,
                    'schema': {
                        'fields': schema_fields
                    },
                    'sourceUris': [source_uri]
                }
            }
        }

        response = self._jobs.insert(
            projectId=write_project_id,
            body=request_body
        ).execute()

        if wait_finish:
            self.poll_job_status(response)

            m, s = divmod((time.time() - start_time), 60)
            timeTaken = '%02d Minutes %02d Seconds' % (m, s)

            logging_string = '%s uploaded to %s (%s)' % (
                    source_uri,
                    '%s:%s:%s' % (write_project_id, write_dataset_id, write_table_id),
                    timeTaken
                )

            if print_details:
                print '\t%s' % logging_string

            if self._logger is not None:
                self._logger.info(logging_string)
        else:
            return response

    def export_to_gcs(self,
                      read_project_id,
                      read_dataset_id,
                      read_table_id,
                      destinationUri,
                      compression='NONE',
                      destinationFormat='CSV',
                      print_details=True,
                      wait_finish=True):

        start_time = time.time()

        request_body = {
            'jobReference': {
                'projectId': read_project_id,
                'job_id': str(uuid.uuid4())
            },

            'configuration': {
                'extract': {
                    'sourceTable': {
                        'projectId': read_project_id,
                        'datasetId': read_dataset_id,
                        'tableId': read_table_id
                    },
                    'destinationUris': [destinationUri],
                    'destinationFormat': destinationFormat,
                    'compression': compression
                }
            }
        }

        response = self._jobs.insert(
            projectId=read_project_id,
            body=request_body
        ).execute()

        if wait_finish:
            self.poll_job_status(response)

            m, s = divmod((time.time() - start_time), 60)
            timeTaken = '%02d Minutes %02d Seconds' % (m, s)

            logging_string = '%s extracted to %s (%s)' % (
                    '%s:%s:%s' % (read_project_id, read_dataset_id, read_table_id),
                    destinationUri,
                    timeTaken
                )

            if print_details:
                print '\t%s' % logging_string

            if self._logger is not None:
                self._logger.info(logging_string)
        else:
            return response

    def load_from_json(self,
                       writeData,
                       json_string,
                       writeDisposition='WRITE_TRUNCATE',
                       print_details=True,
                       wait_finish=True):

        start_time = time.time()

        # projectId, datasetId, tableId, schemaFields must be filled for load jobs
        write_project_id = writeData['projectId']
        write_dataset_id = writeData['datasetId']
        write_table_id = writeData['tableId']
        schema_fields = writeData['schemaFields']

        request_body = {
            'jobReference': {
                'projectId': write_project_id,
                'job_id': str(uuid.uuid4())
            },

            'configuration': {
                'load': {
                    'destinationTable': {
                        'projectId': write_project_id,
                        'datasetId': write_dataset_id,
                        'tableId': write_table_id
                    },
                    'writeDisposition': writeDisposition,
                    'schema': {
                        'fields': schema_fields
                    },
                    'sourceFormat': 'NEWLINE_DELIMITED_JSON'
                }
            }
        }

        media_body = MediaInMemoryUpload(json_string, mimetype='application/octet-stream')

        response = self._jobs.insert(
                body=request_body,
                projectId=write_project_id,
                media_body=media_body
        ).execute()

        if wait_finish:
            self.poll_job_status(response)

            m, s = divmod((time.time() - start_time), 60)
            timeTaken = '%02d Minutes %02d Seconds' % (m, s)

            logging_string = 'Uploaded to %s (%s)' % (
                    '%s:%s:%s' % (write_project_id, write_dataset_id, write_table_id),
                    timeTaken
                )

            if print_details:
                print '\t%s' % logging_string

            if self._logger is not None:
                self._logger.info(logging_string)
        else:
            return response
