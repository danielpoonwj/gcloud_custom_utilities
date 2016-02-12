# Utilities for Google Cloud Platform services

This package contains wrappers for various Cloud Platform services, specifically through Google Compute Engine.
**NOTE** Authorization credentials are read through oauth2client's GoogleCredentials.get_application_default(), and is assumed that the user has full API access for the required services.

## Getting Started

Clone this repo and run python setup.py install.

## Features

**Google Cloud Utilities:**
Google BigQuery
Google Cloud Storage
Google Adwords (WIP)

**Misc Tools:**
Git methods for cloning and fetching
Mail for sending logs via Gmail

## Initializing Utility objects

Each service available has to be initialized before proceeding, which performs the necessary authorization.
>> from gcloud_custom_utilities import BigqueryUtility
>> bq_obj = BigqueryUtility()

This allows functions specific to that service instance to be called without having to reinitialize a service object.
>> project_list = bq_obj.list_projects() # list of project metadata
>> dataset_list = [bq_obj.list_datasets(x['id']) for x in project_list] # gets list of datasets for each project without reinitializing the service object
