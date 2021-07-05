import os
import urllib
import pandas as pd
import json
import tempfile
from datetime import datetime
from dateutil.relativedelta import relativedelta
from airflow.hooks.http_hook import HttpHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from .helpers import slugify


AIRTABLE_TABLE_ID = os.environ.get("AIRTABLE_TABLE_ID", "appWzDISwYl2XFcEz")
S3_REGION = os.environ.get("S3_REGION", "eu-west-1")
S3_BUCKET = os.environ.get("S3_BUCKET", "luko-data-eng-exercice")
S3_PREFIX = os.environ.get("S3_PREFIX", "trung")


class AirtableHook(HttpHook):
    def __init__(self, base_id, **kwargs):
        self.base_id = base_id
        super().__init__(**kwargs)

    def get_conn(self, headers=None):
        session = super().get_conn(headers)
        conn = self.get_connection(self.http_conn_id)
        session.headers.update({"Authorization": f"Bearer {conn.password}"})
        return session

    def run(self, table, date_, offset=None, headers=None):
        endpoint = f"{self.base_id}/{urllib.parse.quote(table)}"
        created_filter_param = (
            "and("
            'is_after(CREATED_AT, "{}"),'
            'is_before(CREATED_AT, "{}")'
            ")".format(date_.isoformat(), (date_ + relativedelta(days=1)).isoformat())
        )
        data = {
            "filterByFormula": created_filter_param,
            "offset": offset,
        }
        return super().run(endpoint, data=data, headers=headers)


def get_all_records(table, json_columns=None, columns=None, **kwargs):
    if json_columns is None:
        json_columns = []
    date_ = kwargs["execution_date"].date()
    hook = AirtableHook(
        AIRTABLE_TABLE_ID, http_conn_id="airtable_default", method="GET"
    )
    s3_hook = S3Hook(aws_conn_id="s3_default")
    resp = hook.run(table, date_)
    data = resp.json()
    records = data["records"]
    offset = data.get("offset")
    while offset:
        resp = hook.run(table, date_, offset)
        data = resp.json()
        records += data["records"]
        offset = data.get("offset")

    with tempfile.TemporaryDirectory() as temp_dir:
        file_name = slugify(table) + ".parquet"
        temp_file = os.path.join(temp_dir, file_name)
        df = pd.DataFrame([record["fields"] for record in records]).rename(
            lambda x: str(x).lower(), axis=1
        )
        for col in json_columns:
            if col in df:
                df[col] = df[col].apply(json.loads).apply(json.dumps)
        if isinstance(columns, (list, tuple)):
            for col in columns:
                if col not in df:
                    df[col] = pd.Series(None, dtype=pd.StringDtype())
            df = df[columns]
        df["created_at"] = pd.to_datetime(df["created_at"])
        df.to_parquet(temp_file, index=False)

        s3_path = os.path.join(S3_PREFIX, date_.strftime("%Y%m%d"), file_name)
        s3_hook.load_file(temp_file, s3_path, S3_BUCKET, replace=True)
