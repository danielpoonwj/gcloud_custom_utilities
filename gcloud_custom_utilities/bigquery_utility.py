import humanize
import uuid
import pandas as pd
import time

from oauth2client.client import GoogleCredentials
from googleapiclient.discovery import build
from googleapiclient.errors import Error
from googleapiclient.http import MediaInMemoryUpload
pd.set_option('expand_frame_repr', False)


def read_string_from_file(read_path):
    with open(read_path, 'rb') as read_file:
        read_string = read_file.read()
    return read_string


class BigqueryUtility:

    def __init__(self):
        credentials = GoogleCredentials.get_application_default()
        service = build('bigquery', 'v2', credentials=credentials)

        self.__service = service
        self.__datasets = self.__service.datasets()
        self.__jobs = self.__service.jobs()
        self.__projects = self.__service.projects()
        self.__tabledata = self.__service.tabledata()
        self.__tables = self.__service.tables()

    def list_projects(self, max_results=None):
        project_list = []

        response = self.__projects.list(
            maxResults=max_results
        ).execute()

        project_list += response['projects']

        while 'nextPageToken' in response and max_results is None:
            page_token = None
            if 'nextPageToken' in response:
                page_token = response['nextPageToken']

            response = self.__projects.list(
                pageToken=page_token
            ).execute()

            if 'projects' in response:
                project_list += response['projects']

        return project_list

    def list_datasets(self, project_id, show_all=False, max_results=None):
        dataset_list = []

        response = self.__datasets.list(
            projectId=project_id,
            all=show_all,
            maxResults=max_results
        ).execute()

        dataset_list += response['datasets']

        while 'nextPageToken' in response and max_results is None:
            page_token = None
            if 'nextPageToken' in response:
                page_token = response['nextPageToken']

            response = self.__datasets.list(
                projectId=project_id,
                all=show_all,
                pageToken=page_token
            ).execute()

            if 'datasets' in response:
                dataset_list += response['datasets']

        return dataset_list

    def list_tables(self, project_id, dataset_id, max_results=None):
        table_list = []

        response = self.__tables.list(
            projectId=project_id,
            datasetId=dataset_id,
            maxResults=max_results
        ).execute()

        table_list += response['tables']

        while 'nextPageToken' in response and max_results is None:
            page_token = None
            if 'nextPageToken' in response:
                page_token = response['nextPageToken']

            response = self.__tables.list(
                projectId=project_id,
                datasetId=dataset_id,
                pageToken=page_token
            ).execute()

            if 'tables' in response:
                table_list += response['tables']

        return table_list

    def get_table_info(self, project_id, dataset_id, table_id):
        return self.__tables.get(
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

    def __get_query_schema(self, response):

        response = self.__jobs.getQueryResults(
                projectId=response['jobReference']['projectId'],
                jobId=response['jobReference']['jobId'],
                maxResults=0
            ).execute()

        return response['schema']['fields']

    def delete_table(self, project_id, dataset_id, table_id):
        self.__tables.delete(
            projectId=project_id,
            datasetId=dataset_id,
            tableId=table_id
        ).execute()
        print '\tDeleted %s:%s:%s' % (project_id, dataset_id, table_id)

    def query(self, project_id, query, async=False, async_data=None, udfInlineCode=None, return_type='list', print_details=True):
        """Submit a query to bigquery. Users can choose whether to submit an
        asynchronous or synchronous query (default)."""
        if async:
            try:
                write_project_id = async_data['projectId']
                write_dataset_id = async_data['datasetId']
                write_table_id = async_data['tableId']
            except (TypeError, KeyError):
                print 'projectId, datasetId and tableId must be filled for async queries'
                raise

            return self.__async_query(project_id, query, write_project_id, write_dataset_id, write_table_id, udfInlineCode, return_type, print_details)
        else:
            if udfInlineCode is not None:
                print 'WARNING: UDF is not enabled for sync queries, please use async if UDF is required'
            return self.__sync_query(project_id, query, return_type, print_details)

    def __sync_query(self, project_id, query, return_type, print_details):
        request_body = {
            'query': query,
            'timeoutMs': 0
        }

        response = self.__jobs.query(
            projectId=project_id,
            body=request_body
        ).execute()

        return self.__iterate_job_results(response, return_type, print_details)

    def __async_query(self, project_id, query, write_project_id, write_dataset_id, write_table_id, udfInlineCode, return_type, print_details):

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

        response = self.__jobs.insert(
            projectId=project_id,
            body=request_body
        ).execute()

        return self.__iterate_job_results(response, return_type, print_details)

    def __iterate_job_results(self, response, returnType, print_details):
        start_time = time.time()

        self.__poll_job_status(response)

        returnList = []

        job_reference = response['jobReference']
        isComplete = False

        while not isComplete or 'pageToken' in response:
            page_token = None
            if 'pageToken' in response:
                page_token = response['pageToken']

            response = self.__jobs.getQueryResults(
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

        m, s = divmod((time.time() - start_time), 60)
        timeTaken = '%02d Minutes %02d Seconds' % (m, s)

        if print_details:
            print '\tData retrieved with %d rows and %s processed (%s)' % (
                int(response['totalRows']),
                humanize.naturalsize(int(response['totalBytesProcessed'])),
                timeTaken
            )

        if returnType == 'list':
            return returnList

        elif returnType == 'dataframe':
            querySchema = self.__get_query_schema(response)

            def __convert_timestamp(input_value):
                try:
                    return pd.datetime.utcfromtimestamp(float(input_value))
                except TypeError:
                    return pd.np.NaN

            dtypeConversion = {
                'TIMESTAMP': lambda x: __convert_timestamp(x)
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

    def __poll_job_status(self, response):
        status_state = None
        while not status_state == 'DONE':
            response = self.__jobs.get(
                jobId=response['jobReference']['jobId'],
                projectId=response['jobReference']['projectId']
            ).execute()

            status_state = response['status']['state']

        if 'errorResult' in response['status']:
            err_msg = '%s: %s' % (response['status']['errorResult']['reason'], response['status']['errorResult']['message'])
            raise Error(err_msg)

    def write_table(self, project_id, query, writeData, writeDisposition='WRITE_TRUNCATE', udfInlineCode=None, print_details=True):
        start_time = time.time()

        try:
            write_project_id = writeData['projectId']
            write_dataset_id = writeData['datasetId']
            write_table_id = writeData['tableId']
        except (TypeError, KeyError):
            print 'projectId, datasetId and tableId must be filled when writing to table'
            raise

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

        response = self.__jobs.insert(
            projectId=project_id,
            body=request_body
        ).execute()

        self.__poll_job_status(response)

        response = self.__jobs.getQueryResults(
            projectId=project_id,
            jobId=response['jobReference']['jobId']
        ).execute()

        m, s = divmod((time.time() - start_time), 60)
        timeTaken = '%02d Minutes %02d Seconds' % (m, s)

        if print_details:
            print '\tQuery %s to %s with %d rows and %s processed (%s)' % (
                'appended' if writeDisposition == 'WRITE_APPEND' else 'written',
                '%s:%s:%s' % (write_project_id, write_dataset_id, write_table_id),
                int(response['totalRows']),
                humanize.naturalsize(int(response['totalBytesProcessed'])),
                timeTaken
            )

    def load_from_gcs(self, writeData, writeDisposition='WRITE_TRUNCATE', skipHeader=True, print_details=True):
        start_time = time.time()
        try:
            write_project_id = writeData['projectId']
            write_dataset_id = writeData['datasetId']
            write_table_id = writeData['tableId']
            schema_fields = writeData['schemaFields']
            source_uri = writeData['sourceUri']
        except (TypeError, KeyError):
            print 'projectId, datasetId, tableId, schemaFields, sourceUri must be filled for load jobs'
            raise

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

        response = self.__jobs.insert(
            projectId=write_project_id,
            body=request_body
        ).execute()

        self.__poll_job_status(response)

        m, s = divmod((time.time() - start_time), 60)
        timeTaken = '%02d Minutes %02d Seconds' % (m, s)

        if print_details:
            print '\t%s uploaded to %s (%s)' % (
                source_uri,
                '%s:%s:%s' % (write_project_id, write_dataset_id, write_table_id),
                timeTaken
            )

    def export_to_gcs(self, read_project_id, read_dataset_id, read_table_id, destinationUri, compression='NONE', destinationFormat='CSV', print_details=True):
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

        response = self.__jobs.insert(
            projectId=read_project_id,
            body=request_body
        ).execute()

        self.__poll_job_status(response)

        m, s = divmod((time.time() - start_time), 60)
        timeTaken = '%02d Minutes %02d Seconds' % (m, s)

        if print_details:
            print '\t%s extracted to %s (%s)' % (
                '%s:%s:%s' % (read_project_id, read_dataset_id, read_table_id),
                destinationUri,
                timeTaken
            )

    def load_from_json(self, writeData, json_string, writeDisposition='WRITE_TRUNCATE', print_details=True, wait_finish=True):
        start_time = time.time()
        try:
            write_project_id = writeData['projectId']
            write_dataset_id = writeData['datasetId']
            write_table_id = writeData['tableId']
            schema_fields = writeData['schemaFields']
        except (TypeError, KeyError):
            print 'projectId, datasetId, tableId, schemaFields must be filled for load jobs'
            raise

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

        response = self.__jobs.insert(
                body=request_body,
                projectId=write_project_id,
                media_body=media_body
        ).execute()

        if wait_finish:
            self.__poll_job_status(response)

            m, s = divmod((time.time() - start_time), 60)
            timeTaken = '%02d Minutes %02d Seconds' % (m, s)

            if print_details:
                print '\tUploaded to %s (%s)' % (
                    '%s:%s:%s' % (write_project_id, write_dataset_id, write_table_id),
                    timeTaken
                )
