import os
import tempfile
import pandas as pd

from airflow.providers.amazon.aws.hooks.s3 import S3Hook


S3_BUCKET = os.environ.get("S3_BUCKET", "luko-data-eng-exercice")
S3_PREFIX = os.environ.get("S3_PREFIX", "trung")


def test_raw_events(file_name, columns=None, **kwargs):
    date_ = kwargs["execution_date"].date()
    s3_hook = S3Hook(aws_conn_id="s3_default")

    s3_path = os.path.join(S3_PREFIX, date_.strftime("%Y%m%d"), file_name)
    with tempfile.TemporaryDirectory() as temp_dir:
        local_temp_file = s3_hook.download_file(s3_path, S3_BUCKET, temp_dir)
        df = pd.read_parquet(os.path.join(temp_dir, local_temp_file))

    assert df.shape[0] > 0
    if columns is None:
        columns = []
    for col in columns:
        assert col in df
