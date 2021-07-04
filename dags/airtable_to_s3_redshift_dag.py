from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pendulum

from functools import partial

from dag_libs.helpers import slugify
from dag_libs.airtable import get_all_records


REDSHIFT_SCHEMA = "trung"

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
    setup_create_tables = [
        PostgresOperator(
            sql="create table if not exists {}({})".format(
                table, ", ".join(f"{column} {type_}" for column, type_ in columns)
            ),
            postgres_conn_id='redshift_default',
            task_id=f'setup_create_table_{table}',
        )
        for table, columns in REDSHIFT_TABLES.items()
    ]
    get_events = [
        PythonOperator(
            python_callable=partial(get_all_records, table=table),
            task_id=f"get_{slugify(table)}_event",
        )
        for table in AIRTABLE_TABLES
    ]
