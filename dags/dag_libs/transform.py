import os
import tempfile
import io
import json
import pandas as pd

from airflow.providers.amazon.aws.hooks.s3 import S3Hook


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
                utm_tags = utm_tags[utm_tags["tag"].str.match(r"^utm_\w+$")].pivot(
                    columns="tag", values="value"
                )
                df = df.merge(utm_tags, "left", left_index=True, right_index=True)

    with io.BytesIO() as f:
        df.to_parquet(f)
        s3_path_out = os.path.join(S3_PREFIX, date_.strftime("%Y%m%d"), out_file_name)
        s3_hook.load_bytes(f.getvalue(), s3_path_out, S3_BUCKET, replace=True)
