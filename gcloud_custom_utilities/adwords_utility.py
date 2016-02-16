from googleads import adwords
from datetime import datetime


class AdwordsUtility:
    def __init__(self, client_customer_id=None, service_version='v201601', credential_path=None):
        # Initialize client object.
        self._client = adwords.AdWordsClient.LoadFromStorage(credential_path)

        if self._client.client_customer_id is None:
            if client_customer_id is not None:
                self.change_client_customer_id(client_customer_id)
            else:
                raise KeyError('client_customer_id has to be filled in googleads.yaml or input as an argument')

        self._service_version = service_version

        self._PAGE_SIZE = 500

    def change_client_customer_id(self, client_customer_id):
        self._client.SetClientCustomerId(client_customer_id)

    def _parse_object(self, fields, input_object, output_type):
        assert output_type in ('object', 'dict', 'list')

        # lowered first letter - align with attributes in objects
        fields = [field[:1].lower() + field[1:] for field in fields]

        if output_type == 'object':
            return input_object

        if output_type == 'list':
            return [input_object[field] for field in fields]

        if output_type == 'dict':
            return {field: input_object[field] for field in fields}

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

    def list_accounts(self, fields=None, predicates=None, output_type='object'):

        assert isinstance(fields, list) if fields is not None else True
        assert isinstance(predicates, list) if predicates is not None else True

        service = self._client.GetService('ManagedCustomerService', version=self._service_version)

        # Default values
        fields = ['Name', 'CustomerId'] if fields is None else fields
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

        account_list = self._iterate_pages(service, selector, output_type)
        return account_list

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

    def download_report(
            self,
            report_type,
            fields,
            start_date,
            end_date,
            write_file,
            download_format='CSV',
            skip_report_header=True,
            skip_report_summary=True,
            skip_column_header=False,
            include_zero_impressions=False):

        assert isinstance(start_date, datetime)
        assert isinstance(end_date, datetime)

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
                }
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
            download_format='CSV',
            skip_report_header=True,
            skip_report_summary=True,
            skip_column_header=False,
            include_zero_impressions=False):

        assert isinstance(start_date, datetime)
        assert isinstance(end_date, datetime)

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
                }
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
