# BigQuery Data Hashing

## Overview

It is a common problem in data projects that a development environment needs to be populated with either dummy data
or a hashed version of data from the production environment. The reasons for doing this are:

- Data is needed upon which to test certain data workflows or other logic before deploying to production
- Production environments are usually more secure than development environments which makes it undesirable to simply 
copy real data to a development environment

The idea of the script in this repo is to leverage [BigQuery's built-in hashing functions](https://cloud.google.com/bigquery/docs/reference/standard-sql/hash_functions) 
in order to hash production data and use it to populate a development environment.

## Usage

Show help:

`python hash-datasets.py --help`

Run hashing script:

`python hash-datasets.py --source_project SOURCE_PROJECT --target_project TARGET_PROJECT --datasets "[DATASET_1,DATASET_2]" --write_disposition WRITE_TRUNCATE`

After running the script, you can view the query jobs in the GCP Cloud Console in BigQuery -> Query History. As 
sometimes the queries can take several minutes, the script does not wait for them to succeed before exiting. The 
status of the jobs can be monitored in the Cloud Console. 

## Notes

- I benchmarked running the script against several tables which contained >300M rows and were around 130GB in size 
(in BigQuery storage). For these tables the queries executed successfully in around 2 minutes. 
- The script will use the same dataset names from the source GCP project as target dataset names in the target 
GCP project. An error will be thrown if these don't exist.
- Data types for fields which are hashed are preserved. eg. A FLOAT type will be casted to a FLOAT type after being 
passed through the hashing function
- Data types included are \[INTEGER, FLOAT, NUMERIC, STRING, BYTES\]. Columns of other data types will not be altered, 
the original value will be outputted in these fields.
- The user/service account which will run this code must have read permissions on the source datasets in BigQuery and 
also write permissions on the target datasets in BigQuery
- This script can easily be ran in an Airflow DAG if there's a desire to populate a target environment regularly with 
data