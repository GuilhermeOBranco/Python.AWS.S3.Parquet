import io
import boto3
import progressbar
from boto3.s3.transfer import TransferConfig
import gzip


class aws_utils:
    def __init__(self):
        MAXIMIUM_FILE_SIZE_MB = 10
        self.config = TransferConfig(multipart_threshold=1024 * MAXIMIUM_FILE_SIZE_MB,
                                     max_concurrency=20,
                                     multipart_chunksize=1024 * MAXIMIUM_FILE_SIZE_MB,
                                     use_threads=True)
        self.s3_client = boto3.client('s3',
                                      aws_access_key_id='',
                                      aws_secret_access_key='')

    def upload(self, bucket, key, file):
        self.s3_client.upload_fileobj(
            Fileobj=file, Bucket=bucket, Key=key, Config=self.config)
