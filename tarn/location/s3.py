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
        self.hash = None
        self.key_size = None

    def contents(self) -> Iterable[Tuple[Key, Any, Meta]]:
        paginator = self.s3.get_paginator('list_objects_v2')
        response_iterator = paginator.paginate(Bucket=self.bucket)
        for response in response_iterator:
            if 'Contents' in response:
                for obj in response['Contents']:
                    yield bytes(obj['Key']), self, None

    @contextmanager
    def read(self, key: Key, return_labels: bool) -> ContextManager[Union[None, Value, Tuple[Value, MaybeLabels]]]:
        try:
            s3_object = self.s3.get_object(Bucket=self.bucket, Key=str(key))
            s3_object_body = s3_object.get('Body')
            if return_labels:
                yield s3_object_body, self._get_labels(str(key))
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
        try:
            self.s3.get_object(Bucket=self.bucket, Key=str(key))
            yield self.s3.get_object(Bucket=self.bucket, Key=str(key))
            self._update_labels(str(key), labels)
            return
        except ClientError as e:
            if e.response['Error']['Code'] == "404" or e.response['Error']['Code'] == "NoSuchKey":
                self.s3.put_object(Bucket=self.bucket, Key=str(key), Body=value)
                yield self.s3.get_object(Bucket=self.bucket, Key=str(key)).get('Body')
                self._update_labels(str(key), labels)
                return
            else:
                raise

    def delete(self, key: Key):
        self.s3.delete_object(Bucket=self.bucket, Key=str(key))

    def _update_labels(self, file: str, labels: MaybeLabels):
        if labels is not None:
            tags = [{'Key': label, 'Value': label} for label in labels]
            self.s3.put_object_tagging(Bucket=self.bucket, Key=file, Tagging={'TagSet': tags})

    def _get_labels(self, file: str) -> MaybeLabels:
        labels_dicts = self.s3.get_object_tagging(Bucket=self.bucket, Key=file)['TagSet']
        return [labels_dict['Key'] for labels_dict in labels_dicts]
