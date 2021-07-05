import os
import urllib
import tempfile
import io
import json
import pandas as pd

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook


S3_BUCKET = os.environ.get("S3_BUCKET", "luko-data-eng-exercice")
S3_PREFIX = os.environ.get("S3_PREFIX", "trung")


def parse_web_events(**kwargs):
    date_ = kwargs["execution_date"].date()
    s3_hook = S3Hook(aws_conn_id="s3_default")
    file_name = "web_events.parquet"
    out_file_name = "web_events_parsed.parquet"

    s3_path = os.path.join(S3_PREFIX, date_.strftime("%Y%m%d"), file_name)
    with tempfile.TemporaryDirectory() as temp_dir:
        local_temp_file = s3_hook.download_file(s3_path, S3_BUCKET, temp_dir)
        df = pd.read_parquet(os.path.join(temp_dir, local_temp_file))
    df = df.merge(
        df["metadata"].apply(json.loads).apply(pd.Series),
        left_index=True,
        right_index=True,
    )
    if "page_search" in df.columns:
        mask_utm = df["page_search"].str.contains(r"\butm_\w+=").fillna(False)
        if mask_utm.any():
            utm_tags = (
                df.loc[mask_utm, "page_search"]
                .str.replace(r"^\?", "")
                .str.split("&")
                .explode()
                .str.split("=", n=1, expand=True)
            )
            utm_tags.columns = ["tag", "value"]
            utm_tags["value"] = utm_tags["value"].apply(urllib.parse.unquote)
            utm_tags = (
                utm_tags[utm_tags["tag"].str.match(r"^utm_\w+$")]
                .reset_index()
                .drop_duplicates(["index", "tag"])  # duplicated tags might be treated differently
                .pivot(index="index", columns="tag", values="value")
            )
            df = df.merge(utm_tags, "left", left_index=True, right_index=True)

    with io.BytesIO() as f:
        df.to_parquet(f)
        s3_path_out = os.path.join(S3_PREFIX, date_.strftime("%Y%m%d"), out_file_name)
        s3_hook.load_bytes(f.getvalue(), s3_path_out, S3_BUCKET, replace=True)


def insert_first_visits_utm_tags(schema, table, **kwargs):
    date_ = kwargs["execution_date"].date()
    s3_hook = S3Hook(aws_conn_id="s3_default")
    redshift_hook = PostgresHook(postgres_conn_id="redshift_default")
    file_name = "web_events_parsed.parquet"

    s3_path = os.path.join(S3_PREFIX, date_.strftime("%Y%m%d"), file_name)
    with tempfile.TemporaryDirectory() as temp_dir:
        local_temp_file = s3_hook.download_file(s3_path, S3_BUCKET, temp_dir)
        df = pd.read_parquet(os.path.join(temp_dir, local_temp_file))

    utm_cols = df.columns[df.columns.str.startswith("utm")]
    df = (
        df.sort_values("created_at")
        .dropna(subset=["user_id"])
        .dropna(subset=utm_cols, how="all")
        .drop_duplicates("user_id")
        .rename({"created_at": "visited_at"}, axis=1)
    )

    db_engine = redshift_hook.get_sqlalchemy_engine()
    with db_engine.connect() as con:
        existed_users = pd.read_sql(
            f"""
            select user_id
            from "{schema}"."{table}"
            where visited_at < %(date)s
        """,
            con=con,
            params={"date": date_},
        )
        if not existed_users.empty:
            df = df[~df["user_id"].isin(existed_users["user_id"])]
        if not df.empty:
            df[["user_id", "visited_at", *utm_cols]].to_sql(
                table, con, schema=schema, if_exists="append", index=False
            )
