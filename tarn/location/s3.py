from contextlib import contextmanager
from typing import Any, ContextManager, Iterable, Tuple, Union

from botocore.exceptions import ClientError

from ..compat import S3Client, Self
from ..config import StorageConfig
from ..digest import key_to_relative
from ..interface import Key, Keys, MaybeLabels, Meta, Value
from .interface import Writable


class S3(Writable):
    def __init__(self, s3_client: S3Client, bucket_name: str):
        self.bucket = bucket_name
        self.s3 = s3_client
        config = self._load_config()
        self.levels = config.levels if config is not None else None
        self.hash = config.hash.build() if config is not None else None

    def contents(self) -> Iterable[Tuple[Key, Any, Meta]]:
        paginator = self.s3.get_paginator('list_objects_v2')
        response_iterator = paginator.paginate(Bucket=self.bucket)
        for response in response_iterator:
            if 'Contents' in response:
                for obj in response['Contents']:
                    if len(obj['Key'].split('/')) == len(self.levels):
                        yield obj['Key'], self, None

    @contextmanager
    def read(self, key: Key, return_labels: bool) -> ContextManager[Union[None, Value, Tuple[Value, MaybeLabels]]]:
        if self.levels is None:
            yield
            return
        file = self._key_to_path(key)
        try:
            s3_object = self.s3.get_object(Bucket=self.bucket, Key=file)
            s3_object_body = s3_object.get('Body')
            if return_labels:
                yield s3_object_body, self._get_labels(file)
            else:
                yield s3_object_body
                return
        except ClientError as e:
            if e.response['Error']['Code'] == "404" or e.response['Error']['Code'] == "NoSuchKey":  # file doesn't exist
                yield
                return
            else:
                raise

    def read_batch(self, keys: Keys) -> Iterable[Tuple[Key, Union[Value, MaybeLabels]]]:
        for key in keys:
            with self.read(key, True) as value:
                yield key, value

    @contextmanager
    def write(self, key: Key, value: Value, labels: MaybeLabels) -> ContextManager:
        if self.levels is None:
            yield
            return
        file = self._key_to_path(key)
        try:
            self.s3.get_object(Bucket=self.bucket, Key=file)
            yield self.s3.get_object(Bucket=self.bucket, Key=file)
            self._update_labels(file, labels)
            return
        except ClientError as e:
            if e.response['Error']['Code'] == "404" or e.response['Error']['Code'] == "NoSuchKey":
                self.s3.put_object(Bucket=self.bucket, Key=file, Body=value)
                yield self.s3.get_object(Bucket=self.bucket, Key=file).get('Body')
                self._update_labels(file, labels)
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
            if e.response['Error']['Code'] == "404" or e.response['Error']['Code'] == "NoSuchKey":  # file doesn't exist
                return
            else:
                raise

    def _update_labels(self, file: str, labels: MaybeLabels):
        if labels is not None:
            tags = [{'Key': label, 'Value': label} for label in labels]
            self.s3.put_object_tagging(Bucket=self.bucket, Key=file, Tagging={'TagSet': tags})

    def _get_labels(self, file: str) -> MaybeLabels:
        labels_dicts = self.s3.get_object_tagging(Bucket=self.bucket, Key=file)['TagSet']
        return [labels_dict['Key'] for labels_dict in labels_dicts]

    @property
    def key_size(self):
        return sum(self.levels)
