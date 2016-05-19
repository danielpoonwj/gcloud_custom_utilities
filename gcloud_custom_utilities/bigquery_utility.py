import humanize
import uuid
import pandas as pd
import unicodecsv as csv
from io import BytesIO
import json
from time import sleep
import os
from itertools import chain

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

    io_output = BytesIO()

    if source_format == 'csv':
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
        # can be loaded from file path or string in a json structure
        if isinstance(f, str):
            if os.path.exists(f):
                with open(f, 'rb') as read_file:
                    json_obj = json.load(read_file)
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


class get_schema_from_json:
    def __init__(self):
        self.dtype_conversion = {
            'unicode': 'STRING',
            'str': 'STRING',
            'basestring': 'STRING',
            'int': 'INTEGER',
            'bool': 'BOOLEAN',
            'float': 'FLOAT',
            'long': 'INTEGER'
        }

    def _get_dict_structure(self, d):
        out = {}
        for k, v in d.iteritems():
            if isinstance(v, dict):
                out[k] = self._get_dict_structure(v)
            elif isinstance(v, list):
                out[k] = []
                collapsed_dict = None

                # if mode is repeated, all the distinct attributes of every record should be included in the schema
                for item in v:
                    if isinstance(item, dict):
                        expanded_item = self._get_dict_structure(item)

                        if collapsed_dict is None:
                            collapsed_dict = expanded_item.copy()
                        else:
                            self.merge_dicts(collapsed_dict, expanded_item)
                    else:
                        out[k].append(item)

                out[k].append(collapsed_dict)

            else:
                if v is not None:
                    out[k] = self.dtype_conversion[type(v).__name__]
        return out

    def merge_dicts(self, original_dict, new_dict):
        for k, v in new_dict.iteritems():
            if k in original_dict:
                if isinstance(original_dict[k], dict) and isinstance(v, dict):
                    self.merge_dicts(original_dict[k], v)

                elif isinstance(v, list):
                    for item in v:
                        for original_item in original_dict[k]:
                            self.merge_dicts(original_item, item)
            else:
                original_dict[k] = v

    def structure_to_schema(self, structure):
        schema_list = []
        for k, v in structure.iteritems():
            if isinstance(v, dict):
                schema_list.append(
                    {
                        'name': k,
                        'type': 'RECORD',
                        'fields': self.structure_to_schema(v)
                    }
                )

            elif isinstance(v, list):
                field_list = list(chain.from_iterable([self.structure_to_schema(x) for x in v]))

                schema_list.append(
                    {
                        'name': k,
                        'type': 'RECORD',
                        'mode': 'REPEATED',
                        'fields': field_list
                    }
                )

            else:
                schema_list.append(
                    {
                        'name': k,
                        'type': v
                    }
                )
        return schema_list

    def merge_list(self, record_list, return_type='schema'):
        assert return_type in ('schema', 'structure')

        merged_dict = None

        for record in record_list:
            record_structure = self._get_dict_structure(record)

            if merged_dict is None:
                merged_dict = record_structure.copy()
            else:
                self.merge_dicts(merged_dict, record_structure)

        if return_type == 'schema':
            return self.structure_to_schema(merged_dict)
        else:
            return merged_dict


class BigqueryUtility:
    def __init__(self, logger=None, authentication_type='Default Credentials', credential_file_path=None, user_name=None, client_secret_path=None, max_retries=3):
        if authentication_type == 'Default Credentials':
            # try building from application default
            try:
                credentials = GoogleCredentials.get_application_default()
                service = build('bigquery', 'v2', credentials=credentials)
            except ApplicationDefaultCredentialsError as e:
                print 'Application Default Credentials unavailable. ' \
                      'To set up Default Credentials, download gcloud from https://cloud.google.com/sdk/gcloud/ ' \
                      'and authenticate through gcloud auth login'
                raise e

        elif authentication_type == 'Stored Credentials':
            import httplib2
            from oauth2client.contrib import multistore_file

            OAUTH_SCOPE = 'https://www.googleapis.com/auth/bigquery'

            assert user_name is not None and credential_file_path is not None
            storage = multistore_file.get_credential_storage(
                filename=credential_file_path,
                client_id=user_name,
                user_agent=None,
                scope=OAUTH_SCOPE
            )

            credentials = storage.get()

            if credentials is None or credentials.invalid:
                if client_secret_path is None or not os.path.exists(client_secret_path):
                    raise UnknownClientSecretsFlowError(
                        'Credentials unavailable. Please provide a valid client_secret_path to rerun authentication'
                    )

                from oauth2client.tools import run_flow, argparser

                try:
                    import argparse
                    flags = argparse.ArgumentParser(parents=[argparser]).parse_args()
                except ImportError:
                    flags = None

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
        self._max_retries = max_retries

    def list_projects(self, max_results=None):
        project_list = []
        project_count = 0

        response = self._projects.list().execute(num_retries=self._max_retries)

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
            ).execute(num_retries=self._max_retries)

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
        ).execute(num_retries=self._max_retries)

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
            ).execute(num_retries=self._max_retries)

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
        ).execute(num_retries=self._max_retries)

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
            ).execute(num_retries=self._max_retries)

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
        ).execute(num_retries=self._max_retries)

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
            ).execute(num_retries=self._max_retries)

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
        ).execute(num_retries=self._max_retries)

    def get_table_info(self, project_id, dataset_id, table_id):
        return self._tables.get(
            projectId=project_id,
            datasetId=dataset_id,
            tableId=table_id
        ).execute(num_retries=self._max_retries)

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
            ).execute(num_retries=self._max_retries)

        return response['schema']['fields']

    def delete_table(self, project_id, dataset_id, table_id, print_details=True):
        self._tables.delete(
            projectId=project_id,
            datasetId=dataset_id,
            tableId=table_id
        ).execute(num_retries=self._max_retries)

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
        ).execute(num_retries=self._max_retries)

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
        ).execute(num_retries=self._max_retries)

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
            ).execute(num_retries=self._max_retries)

            isComplete = response['jobComplete']

            if 'rows' in response:
                if page_token is None:
                    returnList.append([item['name'] for item in response['schema']['fields']])

                for row in response['rows']:
                    returnList.append([item['v'] for item in row['f']])

            sleep(sleep_time)

        if return_type == 'list':
            return returnList
        elif return_type == 'dataframe':
            query_schema = self._get_query_schema(response)

            def _convert_timestamp(input_value):
                try:
                    return pd.datetime.utcfromtimestamp(float(input_value))
                except (TypeError, ValueError):
                    return pd.np.NaN

            if len(returnList) > 0:
                with BytesIO() as file_buffer:
                    csv_writer = csv.writer(file_buffer, lineterminator='\n')
                    csv_writer.writerows(returnList)
                    file_buffer.seek(0)

                    timestamp_cols = [x['name'] for x in query_schema if x['type'] == 'TIMESTAMP']

                    if len(timestamp_cols) > 0:
                        return_df = pd.read_csv(
                            file_buffer,
                            parse_dates=timestamp_cols,
                            date_parser=lambda x: _convert_timestamp(x)
                        )
                    else:
                        return_df = pd.read_csv(file_buffer)
                return return_df

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
            ).execute(num_retries=self._max_retries)

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
                ).execute(num_retries=self._max_retries)

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

        elif 'copy' in response['configuration']:

            destination_table = '%s:%s:%s' % (
                response['configuration']['copy']['destinationTable']['projectId'],
                response['configuration']['copy']['destinationTable']['datasetId'],
                response['configuration']['copy']['destinationTable']['tableId']
            )

            if 'sourceTable' in response['configuration']['copy']:
                source_table = '%s:%s:%s' % (
                    response['configuration']['copy']['sourceTable']['projectId'],
                    response['configuration']['copy']['sourceTable']['datasetId'],
                    response['configuration']['copy']['sourceTable']['tableId']
                )
            else:
                source_table = '[%s]' % ', '.join([
                    '%s:%s:%s' % (
                        source_table_resp['projectId'],
                        source_table_resp['datasetId'],
                        source_table_resp['tableId']
                    ) for source_table_resp in response['configuration']['copy']['sourceTables']
                ])

            write_disposition = response['configuration']['copy']['writeDisposition']

            logging_string = '[BigQuery] Copy Job (%s:%s) %s %s to %s (%s)' % (
                    project_id,
                    job_id,
                    'appended' if write_disposition == 'WRITE_APPEND' else 'written',
                    source_table,
                    destination_table,
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
                    'flattenResults': flattenResults
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
        ).execute(num_retries=self._max_retries)

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
            ).execute(num_retries=self._max_retries)
        except HttpError as e:
            if e.resp.status == 409 and overwrite_existing:
                self.delete_table(write_project_id, write_dataset_id, write_table_id, print_details=print_details)
                response = self._tables.insert(
                    projectId=write_project_id,
                    datasetId=write_dataset_id,
                    body=request_body
                ).execute(num_retries=self._max_retries)
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

        quoted_newlines = 'false' if 'allowQuotedNewlines' not in write_data else write_data['allowQuotedNewlines']

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
                    'sourceUris': source_uri if isinstance(source_uri, list) else [source_uri],
                    'allowQuotedNewlines': quoted_newlines
                }
            }
        }

        response = self._jobs.insert(
            projectId=write_project_id,
            body=request_body
        ).execute(num_retries=self._max_retries)

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
        ).execute(num_retries=self._max_retries)

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

        quoted_newlines = 'false' if 'allowQuotedNewlines' not in write_data else write_data['allowQuotedNewlines']

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
                    'skipLeadingRows': 1 if skipHeader and source_format == 'CSV' else None,
                    'allowQuotedNewlines': quoted_newlines
                }
            }
        }

        media_body = MediaInMemoryUpload(load_string, mimetype='application/octet-stream')

        response = self._jobs.insert(
                body=request_body,
                projectId=write_project_id,
                media_body=media_body
        ).execute(num_retries=self._max_retries)

        if wait_finish:
            return self.poll_job_status(response, print_details, sleep_time)
        else:
            return response

    def copy_table(self,
                    write_data,
                    copy_data,
                    writeDisposition='WRITE_TRUNCATE',
                    print_details=True,
                    wait_finish=True,
                    sleep_time=1):

        # projectId, datasetId, tableId must be filled for write_data and copy_data
        required_keys = ['projectId', 'datasetId', 'tableId']
        assert len(write_data.keys()) == 3 and all([key in required_keys for key in write_data.keys()])

        if isinstance(copy_data, list):
            for element in copy_data:
                assert isinstance(element, dict)
                assert len(element.keys()) == 3 and all([key in required_keys for key in element.keys()])
        else:
            assert len(copy_data.keys()) == 3 and all([key in required_keys for key in copy_data.keys()])
            copy_data = [copy_data]

        request_body = {
            'jobReference': {
                'projectId': write_data['projectId'],
                'job_id': str(uuid.uuid4())
            },

            'configuration': {
                'copy': {
                    'destinationTable': write_data,
                    'sourceTables': copy_data,
                    'writeDisposition': writeDisposition
                }
            }
        }

        response = self._jobs.insert(
            projectId=write_data['projectId'],
            body=request_body
        ).execute(num_retries=self._max_retries)

        if wait_finish:
            return self.poll_job_status(response, print_details, sleep_time)
        else:
            return response

    def write_federated_table(self,
                    write_data,
                    source_format='CSV',
                    skipHeader=False,
                    field_delimiter=',',
                    compression=None,
                    overwrite_existing=True,
                    print_details=True):

        assert source_format in ('CSV', 'NEWLINE_DELIMITED_JSON')

        # projectId, datasetId, tableId, schemaFields and sourceUris must be filled when writing federated table
        write_project_id = write_data['projectId']
        write_dataset_id = write_data['datasetId']
        write_table_id = write_data['tableId']
        schema_fields = write_data['schemaFields']
        source_uris = write_data['sourceUris']

        quoted_newlines = 'false' if 'allowQuotedNewlines' not in write_data else write_data['allowQuotedNewlines']

        if not isinstance(source_uris, list):
            source_uris = [source_uris]

        request_body = {
            'tableReference': {
                'projectId': write_project_id,
                'datasetId': write_dataset_id,
                'tableId': write_table_id
            },
            'externalDataConfiguration': {
                'sourceUris': source_uris,
                'schema': {
                    'fields': schema_fields
                },
                'sourceFormat': source_format,
                'compression': None if compression != 'GZIP' else compression,

                'csvOptions': {
                    'skipLeadingRows': 1 if skipHeader and source_format == 'CSV' else None,
                    'fieldDelimiter': field_delimiter if source_format == 'CSV' else None,
                    'allowQuotedNewlines': quoted_newlines
                }
            }
        }

        # error would be raised if table/view already exists, delete first before reinserting
        try:
            response = self._tables.insert(
                projectId=write_project_id,
                datasetId=write_dataset_id,
                body=request_body
            ).execute(num_retries=self._max_retries)
        except HttpError as e:
            if e.resp.status == 409 and overwrite_existing:
                self.delete_table(write_project_id, write_dataset_id, write_table_id, print_details=print_details)
                response = self._tables.insert(
                    projectId=write_project_id,
                    datasetId=write_dataset_id,
                    body=request_body
                ).execute(num_retries=self._max_retries)
            else:
                raise e

        logging_string = '[BigQuery] Federated Table Inserted (%s:%s:%s) from %s' % (
                write_project_id,
                write_dataset_id,
                write_table_id,
                '[%s]' % ', '.join(source_uris)
            )

        if print_details:
            print '\t%s' % logging_string

        if self._logger is not None:
            self._logger.info(logging_string)

        return response

    def update_table_info(self, project_id, dataset_id, table_id, table_description=None, schema_fields=None, print_details=True):
        request_body = {
            'tableReference': {
                'projectId': project_id,
                'datasetId': dataset_id,
                'tableId': table_id
            }
        }

        if table_description is not None:
            request_body['description'] = table_description

        if schema_fields is not None:
            assert isinstance(schema_fields, list)

            fields = self.get_table_info(
                project_id,
                dataset_id,
                table_id
            )['schema']['fields']

            # all fields have to be supplied even with table patch, this method checks and updates original fields
            # this method won't support adding new fields to prevent potentially accidentally adding etc
            # checks that all supplied schema fields are already existing fields
            assert all([schema_field['name'] in [x['name'] for x in fields] for schema_field in schema_fields])

            for schema_field in schema_fields:
                for field in fields:
                    if schema_field['name'] == field['name']:
                        field.update(schema_field)
                        break

            request_body['schema'] = {
                'fields': fields
            }

        response = self._tables.patch(
            projectId=project_id,
            datasetId=dataset_id,
            tableId=table_id,
            body=request_body
        ).execute(num_retries=self._max_retries)

        logging_string = '[BigQuery] Table Patched (%s:%s:%s)' % (
                project_id,
                dataset_id,
                table_id
            )

        if print_details:
            print '\t%s' % logging_string

        if self._logger is not None:
            self._logger.info(logging_string)

        return response
