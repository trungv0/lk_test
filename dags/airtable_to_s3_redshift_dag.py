import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
import pendulum

from functools import partial

from dag_libs.helpers import slugify
from dag_libs.airtable import get_all_records


S3_REGION = os.environ.get("S3_REGION", "eu-west-1")
S3_BUCKET = os.environ.get("S3_BUCKET", "luko-data-eng-exercice")
S3_PREFIX = os.environ.get("S3_PREFIX", "trung")

REDSHIFT_SCHEMA = "trung"
REDSHIFT_EVENT_TABLE = "event"

REDSHIFT_TABLES = {
    # f"{REDSHIFT_SCHEMA}.event":
    f"{REDSHIFT_SCHEMA}.event_sequence": [
        ("id", "varchar"),
        ("previous_event_id", "varchar"),
        ("next_event_id", "varchar"),
    ]
    # f"{REDSHIFT_SCHEMA}.event_metrics":
    # f"{REDSHIFT_SCHEMA}.event_metrics":
}
AIRTABLE_TABLES = ["App events", "Web events"]

raw_columns = [
    "id",
    "created_at",
    "ip_address",
    "device_id",
    "user_id",
    "uuid",
    "event_type",
    "device_type",
    "platform",
    "event_properties",
    "user_email",
    "metadata",
]


default_args = {
    "start_date": pendulum.datetime(2021, 5, 1),
    "tags": ["exercise"],
}

with DAG(
    dag_id="airtable_to_s3_redshift_dag",
    default_args=default_args,
    catchup=False,
    schedule_interval="0 2 * * *",
) as dag:
    get_events = [
        PythonOperator(
            python_callable=partial(get_all_records, table=table, json_columns=["metadata", "event_properties"], columns=raw_columns),
            task_id=f"get_airtable_{slugify(table)}",
        )
        for table in AIRTABLE_TABLES
    ]
    insert_raw_events_redshift = [
        S3ToRedshiftOperator(
            schema=REDSHIFT_SCHEMA,
            table=REDSHIFT_EVENT_TABLE,
            s3_bucket=S3_BUCKET,
            s3_key="trung/{{ ds_nodash }}/" + f"{slugify(table)}.parquet",
            redshift_conn_id="redshift_default",
            aws_conn_id="s3_default",
            copy_options=[
                "format parquet",
            ],
            task_id=f"insert_redshift_raw_{slugify(table)}",
        )
        for table in AIRTABLE_TABLES
    ]

    for t1, t2 in zip(get_events, insert_raw_events_redshift):
        t1 >> t2
