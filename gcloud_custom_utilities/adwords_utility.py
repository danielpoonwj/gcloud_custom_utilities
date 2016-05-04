from googleads import adwords
from datetime import datetime
from ast import literal_eval
from suds.sudsobject import asdict


class AdwordsUtility:
    def __init__(self, client_customer_id=None, service_version='v201603', credential_path=None):
        # Initialize client object.
        self._client = adwords.AdWordsClient.LoadFromStorage(credential_path)

        if self._client.client_customer_id is None:
            if client_customer_id is not None:
                self.change_client_customer_id(client_customer_id)
            else:
                raise KeyError('client_customer_id has to be filled in googleads.yaml or input as an argument')

        # assuming original input is the mcc account id
        self._MCC_ACCOUNT_ID = self._client.client_customer_id

        self._service_version = service_version

        self._PAGE_SIZE = 500

    def change_client_customer_id(self, client_customer_id):
        self._client.SetClientCustomerId(client_customer_id)

    def reset_to_mcc_id(self):
        self._client.SetClientCustomerId(self._MCC_ACCOUNT_ID)

    def recursive_asdict(self, d):
        """Convert Suds object into serializable format."""
        out = {}
        for k, v in asdict(d).iteritems():
            k = k.replace('.', '')
            k = k[0].lower() + k[1:]

            if hasattr(v, '__keylist__'):
                out[k] = self.recursive_asdict(v)
            elif isinstance(v, list):
                out[k] = []
                for item in v:
                    if hasattr(item, '__keylist__'):
                        out[k].append(self.recursive_asdict(item))
                    else:
                        out[k].append(item)
            else:
                try:
                    out[k] = v.encode('utf-8')
                except AttributeError:
                    out[k] = v

        return out

    def _parse_object(self, fields, input_object, output_type):
        assert output_type in ('object', 'dict', 'list')

        # lowered first letter - align with attributes in objects
        fields = [field[:1].lower() + field[1:] for field in fields]

        if output_type == 'object':
            return input_object

        if output_type == 'list':
            return [input_object[field] for field in fields]

        if output_type == 'dict':
            return self.recursive_asdict(input_object)

    def _iterate_pages(self, service, selector, output_type):
        offset = int(selector['paging']['startIndex'])

        return_list = []

        more_pages = True
        while more_pages:
            page = service.get(selector)

            # Compile results
            if 'entries' in page:
                for entry in page['entries']:
                    return_list.append(self._parse_object(selector['fields'], entry, output_type))

            offset += self._PAGE_SIZE
            selector['paging']['startIndex'] = str(offset)
            more_pages = offset < int(page['totalNumEntries'])

        return return_list

    def list_accounts(self, fields=None, predicates=None, account_labels=None, include_hidden=False, include_mcc=False, output_type='object'):

        assert isinstance(fields, list) if fields is not None else True
        assert isinstance(predicates, list) if predicates is not None else True

        service = self._client.GetService('ManagedCustomerService', version=self._service_version)

        # Default values
        fields = ['Name', 'CustomerId'] if fields is None else fields
        predicates = [] if predicates is None else predicates

        if not include_hidden:
            predicates.append(
                {
                    'field': 'ExcludeHiddenAccounts',
                    'operator': 'EQUALS',
                    'values': 'TRUE'
                }
            )

        if not include_mcc:
            predicates.append(
                {
                    'field': 'CanManageClients',
                    'operator': 'EQUALS',
                    'values': 'FALSE'
                }
            )

        if account_labels is not None:
            if not isinstance(account_labels, list):
                account_labels = [account_labels]

            account_label_list = self._list_account_labels()
            account_labels = [x['id'] for x in account_label_list if x['name'] in account_labels]

            predicates.append(
                {
                    'field': 'AccountLabels',
                    'operator': 'CONTAINS_ANY',
                    'values': account_labels
                }
            )

        # Construct selector
        selector = {
            'fields': fields,
            'predicates': predicates,
            'paging': {
                'startIndex': '0',
                'numberResults': str(self._PAGE_SIZE)
            }
        }

        account_list = self._iterate_pages(service, selector, output_type)
        return account_list

    def _list_account_labels(self):

        service = self._client.GetService('AccountLabelService', version=self._service_version)

        # Default values
        fields = ['LabelName', 'LabelId']

        # Construct selector
        selector = {
            'fields': fields,
            'paging': {
                'startIndex': '0',
                'numberResults': str(self._PAGE_SIZE)
            }
        }

        account_label_list = service.get(selector)['labels']
        return account_label_list

    def get_account_overview(self, start_date, end_date, account_labels=None, include_hidden=False):
        """
        Gets account level details from all accounts (with filters) for given date range
        :param start_date: report start date
        :param end_date: report end date
        :param account_labels: provide list of account labels to filter by if applicable
        :param include_hidden: include hidden accounts
        :return: dictionary {date: {account_id: {metrics}}}
        """
        report_type = 'ACCOUNT_PERFORMANCE_REPORT'

        account_list = [account['customerId'] for account in self.list_accounts(account_labels=account_labels, include_hidden=include_hidden)]

        report_fields = [
            ('Date', 'date'),
            ('AccountDescriptiveName', 'account_name'),
            ('ExternalCustomerId', 'account_id'),
            ('Cost', 'cost'),
            ('Impressions', 'impressions'),
            ('Clicks', 'clicks'),
            ('Conversions', 'conversions')
        ]

        report_cleaner = AdwordsReportCleaner(
                self,
                report_type,
                [x[0] for x in report_fields]
        )

        result_list = []
        for account_id in account_list:
            self.change_client_customer_id(account_id)

            result_list += self.download_report_as_string(
                report_type='ACCOUNT_PERFORMANCE_REPORT',
                fields=[x[0] for x in report_fields],
                start_date=start_date,
                end_date=end_date,
                skip_column_header=True,
                include_zero_impressions=True
            ).strip('\n').split('\n')

        import unicodecsv as csv
        from pandas import date_range as get_date_range

        data = csv.reader(result_list)
        data = [report_cleaner.clean_data(row) for row in data]

        header = [x[1] for x in report_fields]
        return_dict = dict()

        date_range = get_date_range(start_date, end_date)
        for process_date in date_range:
            date_key = process_date.strftime('%Y%m%d')
            return_dict.setdefault(date_key, {})

        for row in data:
            temp_dict = dict(zip(header, row))
            row_date = temp_dict.pop('date')
            row_date = datetime.strptime(row_date, '%Y-%m-%d %H:%M:%S').strftime('%Y%m%d')
            row_account_id = temp_dict.pop('account_id')
            return_dict[row_date][row_account_id] = temp_dict

        return return_dict

    def list_campaigns(self, fields=None, predicates=None, output_type='object'):

        assert isinstance(fields, list) if fields is not None else True
        assert isinstance(predicates, list) if predicates is not None else True

        service = self._client.GetService('CampaignService', version=self._service_version)

        # Default values
        fields = [
            'Id',
            'Name',
            'Status',
            'ServingStatus',
            'AdvertisingChannelType',
            'StartDate',
            'EndDate'
        ] if fields is None else fields

        predicates = [] if predicates is None else predicates

        # Construct selector
        selector = {
            'fields': fields,
            'predicates': predicates,
            'paging': {
                'startIndex': '0',
                'numberResults': str(self._PAGE_SIZE)
            }
        }

        campaign_list = self._iterate_pages(service, selector, output_type)
        return campaign_list

    def list_adgroups(self, campaign_id, fields=None, predicates=None, output_type='object'):
        assert isinstance(campaign_id, (int, long))
        assert isinstance(fields, list) if fields is not None else True
        assert isinstance(predicates, list) if predicates is not None else True

        service = self._client.GetService('AdGroupService', version='v201601')

        # Default values
        fields = [
            'Id',
            'Name',
            'CampaignId',
            'CampaignName',
            'Status'
        ] if fields is None else fields

        # Default predicate has to include campaign id filter, additional conditions are added on top
        default_predicate = [
            {
                'field': 'CampaignId',
                'operator': 'EQUALS',
                'values': [campaign_id]
            }
        ]

        predicates = default_predicate if predicates is None else default_predicate + predicates

        # Construct selector
        selector = {
            'fields': fields,
            'predicates': predicates,
            'paging': {
                'startIndex': '0',
                'numberResults': str(self._PAGE_SIZE)
            }
        }

        adgroup_list = self._iterate_pages(service, selector, output_type)
        return adgroup_list

    def get_generic_service(self, service_name, selector, iterate_pages=False, output_type='object'):
        service = self._client.GetService(service_name, version=self._service_version)

        if iterate_pages:
            return self._iterate_pages(service, selector, output_type)
        else:
            return service.get(selector)

    def download_report(
            self,
            report_type,
            fields,
            start_date,
            end_date,
            write_file,
            predicates=None,
            download_format='CSV',
            skip_report_header=True,
            skip_report_summary=True,
            skip_column_header=False,
            include_zero_impressions=False):

        assert isinstance(start_date, datetime)
        assert isinstance(end_date, datetime)
        assert isinstance(predicates, list) if predicates is not None else True

        report_downloader = self._client.GetReportDownloader(version='v201601')

        report = {
            'reportName': '%s %s-%s' % (report_type, start_date.strftime('%Y%m%d'), end_date.strftime('%Y%m%d')),
            'dateRangeType': 'CUSTOM_DATE',
            'reportType': report_type,
            'downloadFormat': download_format,
            'selector': {
                'fields': fields,
                'dateRange': {
                    'min': start_date.strftime('%Y%m%d'),
                    'max': end_date.strftime('%Y%m%d')
                },
                'predicates': [] if predicates is None else predicates
            }
        }

        report_downloader.DownloadReport(
            report, write_file,
            skip_report_header=skip_report_header,
            skip_report_summary=skip_report_summary,
            skip_column_header=skip_column_header,
            include_zero_impressions=include_zero_impressions
        )

    def download_report_as_string(
            self,
            report_type,
            fields,
            start_date,
            end_date,
            predicates=None,
            download_format='CSV',
            skip_report_header=True,
            skip_report_summary=True,
            skip_column_header=False,
            include_zero_impressions=False):

        assert isinstance(start_date, datetime)
        assert isinstance(end_date, datetime)
        assert isinstance(predicates, list) if predicates is not None else True

        report_downloader = self._client.GetReportDownloader(version='v201601')

        report = {
            'reportName': '%s %s-%s' % (report_type, start_date.strftime('%Y%m%d'), end_date.strftime('%Y%m%d')),
            'dateRangeType': 'CUSTOM_DATE',
            'reportType': report_type,
            'downloadFormat': download_format,
            'selector': {
                'fields': fields,
                'dateRange': {
                    'min': start_date.strftime('%Y%m%d'),
                    'max': end_date.strftime('%Y%m%d')
                },
                'predicates': [] if predicates is None else predicates
            }
        }

        return report_downloader.DownloadReportAsString(
            report,
            skip_report_header=skip_report_header,
            skip_report_summary=skip_report_summary,
            skip_column_header=skip_column_header,
            include_zero_impressions=include_zero_impressions
        )

    def get_report_fields(self, report_type):
        report_definition_service = self._client.GetService('ReportDefinitionService', version=self._service_version)

        # Get report fields.
        fields = report_definition_service.getReportFields(report_type)

        return fields


class AdwordsReportCleaner:
    def __init__(self, adwords_obj, report_type, report_fields, additional_cleaning_functions=None):

        assert isinstance(adwords_obj, AdwordsUtility)
        assert isinstance(report_fields, (list, tuple, set))

        self._adwords_obj = adwords_obj
        self._report_type = report_type
        self._report_fields = report_fields

        # input is in the form of a list of dicts
        # [
        #     {
        #         'name': adwords field name
        #         'type': final output type after function has run (can be different from original)
        #         'function': lambda or named functions accepted, only one argument for value
        #     }
        # ]
        self._additional_cleaning_functions = additional_cleaning_functions

        self._field_references = self._adwords_obj.get_report_fields(report_type)

        def get_field_types(report_fields, field_references):
            field_types_dict = {}

            for field_name in report_fields:
                field_type = None

                # default type can be overridden if resulting type is different after cleaning
                if additional_cleaning_functions is not None and field_name in [x['name'] for x in additional_cleaning_functions]:
                    for function_dict in additional_cleaning_functions:
                        if function_dict['name'] == field_name:
                            field_type = function_dict['type']
                            break
                else:
                    for field_reference in field_references:
                        if field_name == field_reference['fieldName']:
                            field_type = field_reference['fieldType']
                            break

                assert field_type is not None
                field_types_dict[field_name] = field_type

            return field_types_dict

        self._field_types = get_field_types(report_fields, self._field_references)

        # default mapping, everything else will be taken as STRING
        self._bq_map = {
            'Money': 'FLOAT',
            'Double': 'FLOAT',
            'Long': 'INTEGER',
            'Integer': 'INTEGER',
            'Date': 'TIMESTAMP'
        }

    def get_bq_schema(self):
        bq_schema = []

        for field_name in self._report_fields:
            try:
                bq_type = self._bq_map[self._field_types[field_name]]
            except KeyError:
                bq_type = 'STRING'

            bq_schema.append({
                'name': field_name,
                'type': bq_type
            })

        return bq_schema

    def _cleaner(self, value, field_name, field_type):
        # manually input cleaning functions take precedence
        if self._additional_cleaning_functions is not None:
            for function_dict in self._additional_cleaning_functions:
                if field_name == function_dict['name']:
                    return function_dict['function'](value)

        if value.strip() == '--':
            return None
        elif 'List' in field_type:
            return ';'.join(literal_eval(value))
        elif field_type == 'Money':
            # Money is returned as micro units, divide and round to 6 dp to avoid representation errors when dividing
            return round(float(value.replace(',', '')) / 1000000.0, 6)
        elif field_type == 'Date':
            return datetime.strptime(value, '%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S')
        elif field_type in self._bq_map:
            if self._bq_map[field_type] == 'FLOAT':
                return float(value.replace(',', ''))
            if self._bq_map[field_type] == 'INTEGER':
                return int(float(value.replace(',', '')))
        else:
            return value

    def clean_data(self, row):
        cleaned_row = []
        for index, value in enumerate(row):
            field_name = self._report_fields[index]
            field_type = self._field_types[field_name]
            cleaned_row.append(self._cleaner(value, field_name, field_type))

        return cleaned_row
