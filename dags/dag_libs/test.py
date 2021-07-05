import os
import tempfile
import pandas as pd

from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def test_raw_events(file_name, s3_bucket, s3_prefix="", columns=None, **kwargs):
    """Simple test on raw events file.

    :param file_name: file name on s3
    :type file_name: str
    :param s3_bucket: s3 bucket
    :type s3_bucket: str
    :param s3_prefix: s3 prefix, defaults to ""
    :type s3_prefix: str, optional
    :param columns: columns to test for existence, defaults to None
    :type columns: List[str], optional
    """    
    date_ = kwargs["execution_date"].date()
    s3_hook = S3Hook(aws_conn_id="s3_default")

    s3_path = os.path.join(s3_prefix, date_.strftime("%Y%m%d"), file_name)
    with tempfile.TemporaryDirectory() as temp_dir:
        local_temp_file = s3_hook.download_file(s3_path, s3_bucket, temp_dir)
        df = pd.read_parquet(os.path.join(temp_dir, local_temp_file))

    assert df.shape[0] > 0
    if columns is None:
        columns = []
    for col in columns:
        assert col in df
