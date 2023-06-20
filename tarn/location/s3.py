from contextlib import contextmanager
from typing import ContextManager, Iterable, Tuple

from botocore.exceptions import ClientError
from mypy_boto3_s3 import S3Client

from .interface import Writable
from ..config import StorageConfig
from ..digest import key_to_relative
from ..interface import Key, Keys, MaybeLabels, MaybeValue, Value


class S3(Writable):
    def __init__(self, s3_client: S3Client, bucket_name: str):
        self.bucket = bucket_name
        self.s3 = s3_client
        config = self._load_config()
        self.levels = config.levels if config is not None else None
        self.hash = config.hash.build() if config is not None else None

    @contextmanager
    def read(self, key: Key) -> ContextManager[MaybeValue]:
        if self.levels is None:
            yield
            return
        file = self._key_to_path(key)
        try:
            s3_object = self.s3.get_object(Bucket=self.bucket, Key=file)
            s3_object_body = s3_object.get('Body')
            yield s3_object_body
            return
        except ClientError as e:
            if e.response['Error']['Code'] == "404" or e.response['Error']['Code'] == "NoSuchKey": # file doesn't exist
                yield
                return
            else:
                raise

    def read_batch(self, keys: Keys) -> Iterable[Tuple[Key, MaybeValue]]:
        for key in keys:
            yield key, self.read(key)

    @contextmanager
    def write(self, key: Key, value: Value, labels: MaybeLabels) -> ContextManager:
        if self.levels is None:
            yield
            return
        file = self._key_to_path(key)
        try:
            self.s3.get_object(Bucket=self.bucket, Key=file)
            self._update_labels(file, labels)
            yield self.s3.get_object(Bucket=self.bucket, Key=file).get('Body')
            return
        except ClientError as e:
            if e.response['Error']['Code'] == "404" or e.response['Error']['Code'] == "NoSuchKey":
                self.s3.put_object(Bucket=self.bucket, Key=file, Body=value)
                self._update_labels(file, labels)
                yield self.s3.get_object(Bucket=self.bucket, Key=file).get('Body')
                return
            else:
                raise

    def delete(self, key: Key):
        if self.levels is None:
            return
        file = self._key_to_path(key)
        self.s3.delete_object(Bucket=self.bucket, Key=file)

    def _key_to_path(self, key: Key):
        return str(key_to_relative(key, self.levels))

    def _load_config(self):
        try:
            return StorageConfig.parse_raw(self.s3.get_object(Bucket=self.bucket, Key='config.yml').get('Body').read())
        except ClientError as e:
            if e.response['Error']['Code'] == "404" or e.response['Error']['Code'] == "NoSuchKey": # file doesn't exist
                return
            else:
                raise

    def _update_labels(self, file: str, labels: MaybeLabels):
        if labels is not None:
            tags = [{'Key': label, 'Value': ''} for label in labels]
            self.s3.put_object_tagging(Bucket=self.bucket, Key=file, Tagging={'TagSet': tags})

    @property
    def key_size(self):
        return sum(self.levels)
    