import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
import pendulum

from functools import partial

from dag_libs.helpers import slugify
from dag_libs.airtable import get_all_records
from dag_libs.test import test_raw_events
from dag_libs.transform import parse_web_events, insert_first_visits_utm_tags


S3_REGION = os.environ.get("S3_REGION", "eu-west-1")
S3_BUCKET = os.environ.get("S3_BUCKET", "luko-data-eng-exercice")
S3_PREFIX = os.environ.get("S3_PREFIX", "trung")

REDSHIFT_SCHEMA = "trung"
REDSHIFT_EVENT_TABLE = "event"
REDSHIFT_EVENT_SEQUENCE_TABLE = "event_sequence"
REDSHIFT_EVENT_METRICS_TABLE = "event_metrics"
REDSHIFT_ATTRIBUTION_TABLE = "attribution"

AIRTABLE_TABLES = {
    "App events": [
        "id",
        "created_at",
        "ip_address",
        "device_id",
        "user_id",
        "uuid",
        "event_type",
        "device_type",
        "platform",
    ],
    "Web events": [
        "id",
        "created_at",
        "ip_address",
        "device_id",
        "user_id",
        "uuid",
        "user_email",
        "metadata",
    ],
}

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
}

with DAG(
    dag_id="airtable_to_s3_redshift_dag",
    default_args=default_args,
    catchup=False,
    schedule_interval="0 2 * * *",
    tags=["exercise"],
) as dag:
    get_events = [
        PythonOperator(
            python_callable=partial(
                get_all_records,
                table=table,
                json_columns=["metadata", "event_properties"],
                columns=raw_columns,
            ),
            task_id=f"get_airtable_{slugify(table)}",
        )
        for table in AIRTABLE_TABLES.keys()
    ]
    test_events = [
        PythonOperator(
            python_callable=partial(
                test_raw_events,
                file_name=f"{slugify(table)}.parquet",
                columns=columns,
            ),
            task_id=f"test_raw_{slugify(table)}",
        )
        for table, columns in AIRTABLE_TABLES.items()
    ]
    parse_web_events_task = PythonOperator(
        python_callable=parse_web_events,
        task_id="parse_web_events",
    )
    remove_existed_raw_events_redshift = PostgresOperator(
        sql=f"delete from {REDSHIFT_SCHEMA}.{REDSHIFT_EVENT_TABLE} "
        "where created_at >= '{{ ds }}' and created_at < '{{ tomorrow_ds }}'",
        postgres_conn_id="redshift_default",
        task_id=f"remove_existed_raw_events_redshift",
    )
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

    truncate_event_sequence_redshift = PostgresOperator(
        sql=f"truncate table {REDSHIFT_SCHEMA}.{REDSHIFT_EVENT_SEQUENCE_TABLE}",
        postgres_conn_id="redshift_default",
        task_id=f"truncate_event_sequence_redshift",
    )

    aggregate_event_sequence_redshift = PostgresOperator(
        sql=f"insert into {REDSHIFT_SCHEMA}.{REDSHIFT_EVENT_SEQUENCE_TABLE}"
        "(id, user_id, previous_id, next_id) "
        "select id, user_id,"
        "lag(id) over (partition by user_id order by created_at) as previous_id,"
        "lead(id) over (partition by user_id order by created_at) as next_id "
        f"from {REDSHIFT_SCHEMA}.{REDSHIFT_EVENT_TABLE} "
        "where user_id is not null "
        "order by created_at",
        postgres_conn_id="redshift_default",
        task_id=f"aggregate_event_sequence_redshift",
    )

    remove_existed_metrics_redshift = PostgresOperator(
        sql=f'delete from "{REDSHIFT_SCHEMA}"."{REDSHIFT_EVENT_METRICS_TABLE}" '
        "where date = '{{ ds }}'",
        postgres_conn_id="redshift_default",
        task_id=f"remove_existed_metrics_redshift",
    )

    aggregate_metrics_redshift = PostgresOperator(
        sql=f'insert into "{REDSHIFT_SCHEMA}"."{REDSHIFT_EVENT_METRICS_TABLE}"'
        "(date, nb_app_events, nb_web_events) "
        "select created_at::date,"
        "count(case when device_type is null and platform is null and event_properties is null then 1 else null end),"
        "count(case when device_type is null and platform is null and event_properties is null then null else 1 end) "
        f'from "{REDSHIFT_SCHEMA}"."{REDSHIFT_EVENT_TABLE}" '
        "where created_at::date = '{{ ds }}' "
        "group by created_at::date",
        postgres_conn_id="redshift_default",
        task_id=f"aggregate_metrics_redshift",
    )

    remove_existed_attribution_redshift = PostgresOperator(
        sql=f"delete from {REDSHIFT_SCHEMA}.{REDSHIFT_ATTRIBUTION_TABLE} "
        "where visited_at >= '{{ ds }}' and visited_at < '{{ tomorrow_ds }}'",
        postgres_conn_id="redshift_default",
        task_id=f"remove_existed_attribution_redshift",
    )

    insert_attribution_redshift = PythonOperator(
        python_callable=partial(
            insert_first_visits_utm_tags,
            schema=REDSHIFT_SCHEMA,
            table=REDSHIFT_ATTRIBUTION_TABLE,
        ),
        task_id="insert_attribution_redshift",
    )

    for t1, t2 in zip(get_events, test_events):
        t1 >> t2
    test_events >> parse_web_events_task
    (
        parse_web_events_task
        >> remove_existed_raw_events_redshift
        >> insert_raw_events_redshift
    )
    (
        insert_raw_events_redshift
        >> truncate_event_sequence_redshift
        >> aggregate_event_sequence_redshift
    )
    (
        insert_raw_events_redshift
        >> remove_existed_metrics_redshift
        >> aggregate_metrics_redshift
    )
    (
        parse_web_events_task
        >> remove_existed_attribution_redshift
        >> insert_attribution_redshift
    )
