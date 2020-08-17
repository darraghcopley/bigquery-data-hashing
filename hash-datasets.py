import logging
import fire
from google.cloud import bigquery


def mask_integer(col):
    return f"FARM_FINGERPRINT(CAST(`{col}` as STRING)) as `{col}`"


def mask_numeric(col):
    return f"CAST(FARM_FINGERPRINT(CAST(`{col}` as STRING)) as NUMERIC) as `{col}`"


def mask_float(col):
    return f"CAST(FARM_FINGERPRINT(CAST(`{col}` as STRING)) as FLOAT64) as `{col}`"


def mask_string(col):
    return f"TO_BASE64(SHA256(`{col}`)) as `{col}`"


def mask_bytes(col):
    return f"SHA256(`{col}`) as `{col}`"


def hash_table(client, table_ref, target_dataset: str, write_disposition: str):
    """
    Create a hashing query to execute in BigQuery and output hashed results to target dataset

    Args:
        client: BigQuery client to use
        table_ref: Table reference object
        target_dataset (str): Target dataset including target project name eg. `gcp_project.dataset_name`
        exclude_cols (list): Columns to exclude from hashing job
        write_disposition (str): How to handle existing destination tables. Accepted values are "WRITE_TRUNCATE", "WRITE_EMPTY", "WRITE_APPEND".

    Returns:
        (tuple) The query job object and the destination table for the hashing operation
    """
    table = client.get_table(table_ref)

    query_field_exps = []

    for field in table.schema:
        if field.field_type == "INTEGER":
            query_field_exps.append(mask_integer(col=field.name))
        elif field.field_type == "NUMERIC":
            query_field_exps.append(mask_numeric(col=field.name))
        elif field.field_type == "FLOAT":
            query_field_exps.append(mask_float(col=field.name))
        elif field.field_type == "STRING":
            query_field_exps.append(mask_string(col=field.name))
        elif field.field_type == "BYTES":
            query_field_exps.append(mask_bytes(col=field.name))
        elif field.field_type == "INTEGER":
            query_field_exps.append(mask_integer(col=field.name))
        else:
            logging.info(f"No masking function for type {field.field_type}")
            query_field_exps.append(field.name)

    masking_query = f'SELECT {", ".join(query_field_exps)} FROM `{table.full_table_id.replace(":", ".")}`'

    destination_table = f"{target_dataset}.{table.table_id}"
    # Set job config w/ write disposition as truncate to overwrite previous results
    job_config = bigquery.QueryJobConfig(
        destination=destination_table, write_disposition=write_disposition
    )

    # Create job
    query_job = client.query(
        query=masking_query, job_config=job_config, location=table.location
    )

    return query_job, destination_table


def hash_tables(client, target_env: str, dataset: str, write_disposition: str):
    """
    Hash all tables in a dataset in current environment and output to target GCP environment

    Args:
        client: Bigquery client to use
        target_env (str): Target GCP Project
        dataset (str): Dataset to hash
        write_disposition (str): How to handle existing destination tables. Accepted values are "WRITE_TRUNCATE", "WRITE_EMPTY", "WRITE_APPEND".

    Returns:
        A list of tables for which there were failures
    """
    tables = client.list_tables(dataset)

    query_jobs = []

    for table in tables:
        if table.table_type == "TABLE":
            try:
                query_job, destination_table = hash_table(
                    client=client,
                    target_dataset=f"{target_env}.{dataset}",
                    table_ref=table.reference,
                    write_disposition=write_disposition,
                )
                query_jobs.append(query_job)
            except Exception as e:
                logging.error(f"Error masking table `{table.full_table_id}`- {e}")

    failures = []
    for query_job in query_jobs:
        if query_job.errors:
            logging.error(
                f"Query job with id {query_job.job_id} and destination {query_job.destination} has errors"
            )
            failures.append({query_job.destination})

    return failures


def hash_datasets(
    source_project: str, target_project: str, datasets: list, write_disposition: str
):
    """
    Hash a list of datasets in source GCP project and output to target GCP project

    Args:
        source_project (str): Source GCP project where input data will be sourced from
        target_project (str): Target GCP project where hashed data will be outputted to
        datasets (list): A List of datasets to hash
        write_disposition (str): How to handle existing destination tables. Accepted values are "WRITE_TRUNCATE", "WRITE_EMPTY", "WRITE_APPEND".
    """
    if source_project == target_project:
        raise Exception(
            f"Source project `{source_project}` and target project `{target_project}` are the same. This is not supported."
        )

    logging.info(
        f"Hashing data from project `{source_project}` and outputting to target project `{source_project}`"
    )

    client = bigquery.Client(project=source_project)

    failures = []
    for dataset in datasets:
        dataset_failures = hash_tables(
            client=client,
            target_env=target_project,
            dataset=dataset,
            write_disposition=write_disposition,
        )
        if dataset_failures:
            failures.extend(dataset_failures)

    if failures:
        raise Exception(
            f"Hashing process had errors for the following tables: {failures}"
        )


if __name__ == "__main__":
    fire.Fire(hash_datasets)
