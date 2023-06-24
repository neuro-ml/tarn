from contextlib import contextmanager
import json
from typing import Any, BinaryIO, ContextManager, Iterable, Tuple, Union

from botocore.exceptions import ClientError

from ..compat import S3Client
from ..interface import Key, Keys, MaybeLabels, Meta, Value
from ..utils import value_to_buffer
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
                    yield bytes.fromhex(obj['Key']), self, None

    @contextmanager
    def read(self, key: Key, return_labels: bool) -> ContextManager[Union[None, Value, Tuple[Value, MaybeLabels]]]:
        try:
            s3_object = self.s3.get_object(Bucket=self.bucket, Key=key.hex())
            s3_object_body = s3_object.get('Body')
            if return_labels:
                yield StreamingBodyBuffer(s3_object_body), self._get_labels(key.hex())
            else:
                yield StreamingBodyBuffer(s3_object_body)
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
        with value_to_buffer(value) as value:
            try:
                self.s3.get_object(Bucket=self.bucket, Key=key.hex())
                yield StreamingBodyBuffer(self.s3.get_object(Bucket=self.bucket, Key=key.hex()).get('Body'))
                self._update_labels(key.hex(), labels)
                return
            except ClientError as e:
                if e.response['Error']['Code'] == "404" or e.response['Error']['Code'] == "NoSuchKey":
                    self.s3.put_object(Bucket=self.bucket, Key=key.hex(), Body=value)
                    yield StreamingBodyBuffer(self.s3.get_object(Bucket=self.bucket, Key=key.hex()).get('Body'))
                    self._update_labels(key.hex(), labels)
                    return
                else:
                    raise

    def delete(self, key: Key):
        self.s3.delete_object(Bucket=self.bucket, Key=key.hex())

    def _update_labels(self, file: str, labels: MaybeLabels):
        if labels is not None:
            tags = [{'Key': label, 'Value': label} for label in labels]
            self.s3.put_object_tagging(Bucket=self.bucket, Key=file, Tagging={'TagSet': tags})

    def _get_labels(self, file: str) -> MaybeLabels:
        labels_dicts = self.s3.get_object_tagging(Bucket=self.bucket, Key=file)['TagSet']
        return [labels_dict['Key'] for labels_dict in labels_dicts]


class StreamingBodyBuffer(BinaryIO):
    def __init__(self, streaming_body):
        super().__init__()
        self._streaming_body = streaming_body
    
    def __getattribute__(self, attr) -> Any:
        streaming_body = super().__getattribute__('_streaming_body')
        return getattr(streaming_body, attr)
