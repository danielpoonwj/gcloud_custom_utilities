import humanize
import uuid
import pandas as pd
from time import sleep
import os

from oauth2client.client import GoogleCredentials, ApplicationDefaultCredentialsError, flow_from_clientsecrets, UnknownClientSecretsFlowError
from googleapiclient.discovery import build
from googleapiclient.errors import Error, HttpError
from googleapiclient.http import MediaInMemoryUpload
pd.set_option('expand_frame_repr', False)


def read_string_from_file(read_path):
    with open(read_path, 'rb') as read_file:
        read_string = read_file.read()
    return read_string


def convert_file_to_string(f, source_format='csv'):
    assert source_format.lower() in ('csv', 'json')

    from io import BytesIO
    io_output = BytesIO()

    if source_format == 'csv':
        import unicodecsv as csv
        string_writer = csv.writer(io_output, lineterminator='\n')

        # file path to .csv
        if isinstance(f, str):
            assert os.path.exists(f)

            with open(f, 'rb') as read_file:
                string_writer.writerows(csv.reader(read_file))

        # also accepts list of lists
        elif any(isinstance(el, list) for el in f):
            string_writer.writerows(f)

        else:
            raise TypeError('Only file path or list of lists accepted')

    elif source_format == 'json':
        import json

        # can be loaded from file path or string in a json structure
        if isinstance(f, str):
            if os.path.exists(f):
                json_obj = json.load(f)
            else:
                json_obj = json.loads(f)

        else:
            try:
                json.dumps(f)
                json_obj = f
            except TypeError as e:
                raise e

        for index, obj in enumerate(json_obj):
            if index < len(json_obj) - 1:
                io_output.write(json.dumps(obj) + '\n')
            else:
                io_output.write(json.dumps(obj))

    return_string = io_output.getvalue()
    io_output.close()

    return return_string


def get_schema_from_dataframe(input_df):
    dtype_df = input_df.dtypes.reset_index(drop=False)
    dtype_df = dtype_df.rename(columns={'index': 'name', 0: 'type'})

    dtype_conversion_dict = {
        'b': 'BOOLEAN',
        'i': 'INTEGER',
        'u': 'INTEGER',
        'f': 'FLOAT',
        'c': 'FLOAT',
        'O': 'STRING',
        'S': 'STRING',
        'U': 'STRING',
        'M': 'TIMESTAMP'
    }

    dtype_df['type'] = dtype_df['type'].map(lambda x: dtype_conversion_dict[x.kind])
    return dtype_df.to_dict('records')


class BigqueryUtility:
    def __init__(self, logger=None, authentication_type='Default Credentials', credential_file_path=None, user_name=None, client_secret_path=None):

        if authentication_type == 'Default Credentials':
            # try building from application default
            try:
                credentials = GoogleCredentials.get_application_default()
                service = build('bigquery', 'v2', credentials=credentials)
            except ApplicationDefaultCredentialsError as e:
                print 'Application Default Credentials unavailable. To set up Default Credentials, download gcloud from https://cloud.google.com/sdk/gcloud/ and authenticate through gcloud auth login'
                raise e

        elif authentication_type == 'Stored Credentials':
            import httplib2
            from oauth2client.contrib import multistore_file
            from oauth2client.tools import run_flow, argparser

            try:
                import argparse
                flags = argparse.ArgumentParser(parents=[argparser]).parse_args()
            except ImportError:
                flags = None

            OAUTH_SCOPE = 'https://www.googleapis.com/auth/bigquery'

            assert user_name is not None and credential_file_path is not None and os.path.exists(credential_file_path)
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

            service = build('bigquery', 'v2', http=http)
        else:
            raise TypeError('Authentication types available are "Default Credentials" and "Stored Credentials"')

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

        if 'projects' in response:
            project_list += response['projects']
            project_count += len(response['projects'])
        else:
            return project_list

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

        if 'jobs' in response:
            job_list += response['jobs']
            job_count += len(response['jobs'])
        else:
            return job_list

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

        if 'datasets' in response:
            dataset_list += response['datasets']
            dataset_count += len(response['datasets'])
        else:
            return dataset_list

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

        if 'tables' in response:
            table_list += response['tables']
            table_count += len(response['tables'])
        else:
            return table_list

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

    def get_sharded_date_range(self, project_id, dataset_id, print_details=True):
        tableList = self.list_tables(project_id, dataset_id, max_results=100000)

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

            if print_details:
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

    def delete_table(self, project_id, dataset_id, table_id, print_details=True):
        self._tables.delete(
            projectId=project_id,
            datasetId=dataset_id,
            tableId=table_id
        ).execute()

        logging_string = '[BigQuery] Deleted %s:%s:%s' % (project_id, dataset_id, table_id)

        if print_details:
            print '\t%s' % logging_string

        if self._logger is not None:
            self._logger.info(logging_string)

    def query(self, project_id, query, async=False, async_data=None, udfInlineCode=None, return_type='list', print_details=True, sleep_time=1):
        """Submit a query to bigquery. Users can choose whether to submit an
        asynchronous or synchronous query (default)."""
        if async:
            # projectId, datasetId and tableId must be filled for async queries
            write_project_id = async_data['projectId']
            write_dataset_id = async_data['datasetId']
            write_table_id = async_data['tableId']

            return self._async_query(project_id, query, write_project_id, write_dataset_id, write_table_id, udfInlineCode, return_type, print_details, sleep_time)
        else:
            if udfInlineCode is not None:
                print 'WARNING: UDF is not enabled for sync queries, please use async if UDF is required'

                if self._logger is not None:
                    self._logger.warn('UDF is not enabled for sync queries, please use async if UDF is required')

            return self._sync_query(project_id, query, return_type, print_details, sleep_time)

    def _sync_query(self, project_id, query, return_type, print_details, sleep_time):
        request_body = {
            'query': query,
            'timeoutMs': 0
        }

        response = self._jobs.query(
            projectId=project_id,
            body=request_body
        ).execute()

        return self._iterate_job_results(response, return_type, print_details, sleep_time)

    def _async_query(self, project_id, query, write_project_id, write_dataset_id, write_table_id, udfInlineCode, return_type, print_details, sleep_time):
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

        return self._iterate_job_results(response, return_type, print_details, sleep_time)

    def _iterate_job_results(self, response, return_type, print_details, sleep_time):
        response = self.poll_job_status(response, print_details, sleep_time)

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

            sleep(1)

        if return_type == 'list':
            return returnList
        elif return_type == 'dataframe':
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

    def poll_job_status(self, response, print_details=True, sleep_time=1):
        status_state = None

        project_id = response['jobReference']['projectId']
        job_id = response['jobReference']['jobId']

        while not status_state == 'DONE':
            response = self._jobs.get(
                jobId=job_id,
                projectId=project_id
            ).execute()

            status_state = response['status']['state']
            sleep(sleep_time)

        if 'errorResult' in response['status']:
            raise Error(response['status']['errorResult'])

        # for logging
        m, s = divmod(
                (
                    pd.datetime.utcfromtimestamp(float(response['statistics']['endTime']) / 1000) -
                    pd.datetime.utcfromtimestamp(float(response['statistics']['creationTime']) / 1000)
                ).seconds, 60)

        time_taken = '%02d Minutes %02d Seconds' % (m, s)

        if 'load' in response['statistics']:

            destination_table = '%s:%s:%s' % (
                response['configuration']['load']['destinationTable']['projectId'],
                response['configuration']['load']['destinationTable']['datasetId'],
                response['configuration']['load']['destinationTable']['tableId']
            )

            write_disposition = response['configuration']['load']['writeDisposition']
            file_size = humanize.naturalsize(int(response['statistics']['load']['inputFileBytes']))
            row_count = int(response['statistics']['load']['outputRows'])

            logging_string = '[BigQuery] Load Job (%s:%s) %s to %s with %d rows and %s processed (%s)' % (
                    project_id,
                    job_id,
                    'appended' if write_disposition == 'WRITE_APPEND' else 'written',
                    destination_table,
                    row_count,
                    file_size,
                    time_taken
                )

        elif 'query' in response['statistics']:
            is_async = bool(response['configuration']['query']['allowLargeResults']) if 'allowLargeResults' in response['configuration']['query'] else False

            query_response = self._jobs.getQueryResults(
                    projectId=response['jobReference']['projectId'],
                    jobId=response['jobReference']['jobId'],
                    maxResults=0
                ).execute()

            file_size = humanize.naturalsize(int(response['statistics']['query']['totalBytesProcessed']))
            row_count = int(query_response['totalRows'])

            if is_async:
                destination_table = '%s:%s:%s' % (
                    response['configuration']['query']['destinationTable']['projectId'],
                    response['configuration']['query']['destinationTable']['datasetId'],
                    response['configuration']['query']['destinationTable']['tableId']
                )

                write_disposition = response['configuration']['query']['writeDisposition']

                logging_string = '[BigQuery] Asynchronous Query Job (%s:%s) %s to %s with %d rows and %s processed (%s)' % (
                        project_id,
                        job_id,
                        'appended' if write_disposition == 'WRITE_APPEND' else 'written',
                        destination_table,
                        row_count,
                        file_size,
                        time_taken
                    )
            else:
                logging_string = '[BigQuery] Synchronous Query Job (%s:%s) returned with %d rows and %s processed (%s)' % (
                        project_id,
                        job_id,
                        row_count,
                        file_size,
                        time_taken
                    )

        elif 'extract' in response['statistics']:

            source_table = '%s:%s:%s' % (
                response['configuration']['extract']['sourceTable']['projectId'],
                response['configuration']['extract']['sourceTable']['datasetId'],
                response['configuration']['extract']['sourceTable']['tableId']
            )

            destination_uris = ', '.join(response['configuration']['extract']['destinationUris'])

            logging_string = '[BigQuery] Extract Job (%s:%s) exported %s to %s (%s)' % (
                    project_id,
                    job_id,
                    source_table,
                    destination_uris,
                    time_taken
                )

        else:
            logging_string = ''

        if print_details:
            print '\t%s' % logging_string

        if self._logger is not None:
            self._logger.info(logging_string)

        return response

    def check_status_from_responses(self, response_list, print_details=True, sleep_time=1):
        assert isinstance(response_list, (list, tuple, set))
        return_list = []
        for response in response_list:
            return_list.append(self.poll_job_status(response, print_details, sleep_time))

        return return_list

    def write_table(self,
                    project_id,
                    query,
                    write_data,
                    writeDisposition='WRITE_TRUNCATE',
                    udfInlineCode=None,
                    print_details=True,
                    wait_finish=True,
                    sleep_time=1):

        # projectId, datasetId and tableId must be filled when writing to table
        write_project_id = write_data['projectId']
        write_dataset_id = write_data['datasetId']
        write_table_id = write_data['tableId']

        flattenResults = 'true' if 'flattenResults' not in write_data else write_data['flattenResults']
        maximumBillingTier = None if 'maximumBillingTier' not in write_data else write_data['maximumBillingTier']

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
                    'writeDisposition': writeDisposition,
                    'flattenResults': flattenResults,
                    'maximumBillingTier': maximumBillingTier
                }
            }
        }

        # error would be raised if overwriting a view, check if exists and delete first
        try:
            existing_table = self.get_table_info(write_project_id, write_dataset_id, write_table_id)
            if existing_table['type'] == 'VIEW':
                self.delete_table(write_project_id, write_dataset_id, write_table_id, print_details=print_details)
        except HttpError as e:
            # table does not exist
            if e.resp.status == 404:
                pass
            else:
                raise e

        response = self._jobs.insert(
            projectId=project_id,
            body=request_body
        ).execute()

        if wait_finish:
            return self.poll_job_status(response, print_details, sleep_time)
        else:
            return response

    def write_view(self,
                    query,
                    write_data,
                    udfInlineCode=None,
                    overwrite_existing=True,
                    print_details=True):

        # projectId, datasetId and tableId must be filled when writing to view
        write_project_id = write_data['projectId']
        write_dataset_id = write_data['datasetId']
        write_table_id = write_data['tableId']

        request_body = {
            'tableReference': {
                'projectId': write_project_id,
                'datasetId': write_dataset_id,
                'tableId': write_table_id
            },
            'view': {
                'userDefinedFunctionResources': [
                    None if udfInlineCode is None else {'inlineCode': udfInlineCode}
                ],
                'query': query
            }
        }

        # error would be raised if table/view already exists, delete first before reinserting
        try:
            response = self._tables.insert(
                projectId=write_project_id,
                datasetId=write_dataset_id,
                body=request_body
            ).execute()
        except HttpError as e:
            if e.resp.status == 409 and overwrite_existing:
                self.delete_table(write_project_id, write_dataset_id, write_table_id, print_details=print_details)
                response = self._tables.insert(
                    projectId=write_project_id,
                    datasetId=write_dataset_id,
                    body=request_body
                ).execute()
            else:
                raise e

        logging_string = '[BigQuery] View Inserted (%s:%s:%s)' % (
                write_project_id,
                write_dataset_id,
                write_table_id
            )

        if print_details:
            print '\t%s' % logging_string

        if self._logger is not None:
            self._logger.info(logging_string)

        return response

    def load_from_gcs(self,
                      write_data,
                      source_format='CSV',
                      field_delimiter=',',
                      skipHeader=True,
                      writeDisposition='WRITE_TRUNCATE',
                      print_details=True,
                      wait_finish=True,
                      sleep_time=1):

        assert source_format in ('CSV', 'NEWLINE_DELIMITED_JSON')

        # projectId, datasetId, tableId, schemaFields, sourceUri must be filled for load jobs
        write_project_id = write_data['projectId']
        write_dataset_id = write_data['datasetId']
        write_table_id = write_data['tableId']
        schema_fields = write_data['schemaFields']
        source_uri = write_data['sourceUri']

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
                    'sourceFormat': source_format,
                    'skipLeadingRows': 1 if skipHeader and source_format == 'CSV' else None,
                    'fieldDelimiter': field_delimiter if source_format == 'CSV' else None,
                    'schema': {
                        'fields': schema_fields
                    },
                    'sourceUris': source_uri if isinstance(source_uri, list) else [source_uri]
                }
            }
        }

        response = self._jobs.insert(
            projectId=write_project_id,
            body=request_body
        ).execute()

        if wait_finish:
            return self.poll_job_status(response, print_details, sleep_time)
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
                      wait_finish=True,
                      sleep_time=1):

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
            return self.poll_job_status(response, print_details, sleep_time)
        else:
            return response

    def load_from_string(self,
                        write_data,
                        load_string,
                        source_format='CSV',
                        skipHeader=True,
                        writeDisposition='WRITE_TRUNCATE',
                        print_details=True,
                        wait_finish=True,
                        sleep_time=1):

        assert source_format in ('CSV', 'NEWLINE_DELIMITED_JSON')

        # projectId, datasetId, tableId, schemaFields must be filled for load jobs
        write_project_id = write_data['projectId']
        write_dataset_id = write_data['datasetId']
        write_table_id = write_data['tableId']
        schema_fields = write_data['schemaFields']

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
                    'sourceFormat': source_format,
                    'skipLeadingRows': 1 if skipHeader and source_format == 'CSV' else None
                }
            }
        }

        media_body = MediaInMemoryUpload(load_string, mimetype='application/octet-stream')

        response = self._jobs.insert(
                body=request_body,
                projectId=write_project_id,
                media_body=media_body
        ).execute()

        if wait_finish:
            return self.poll_job_status(response, print_details, sleep_time)
        else:
            return response
