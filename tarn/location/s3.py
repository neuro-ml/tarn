import warnings
from contextlib import contextmanager
from datetime import datetime
from io import SEEK_CUR, SEEK_SET
from typing import Any, BinaryIO, ContextManager, Iterable, Mapping, Optional, Tuple, Union

from botocore.exceptions import ClientError, ConnectionError

from ..compat import S3Client
from ..digest import key_to_relative, value_to_buffer
from ..exceptions import CollisionError, StorageCorruption
from ..interface import Key, MaybeLabels, Meta, Value
from ..utils import match_buffers
from .interface import Writable


class S3(Writable):
    def __init__(self, s3_client: S3Client, bucket_name: str):
        self.bucket = bucket_name
        self.s3 = s3_client
        self.hash = None

    def contents(self) -> Iterable[Tuple[Key, Any, Meta]]:
        paginator = self.s3.get_paginator('list_objects_v2')
        response_iterator = paginator.paginate(Bucket=self.bucket)
        for response in response_iterator:
            if 'Contents' in response:
                for obj in response['Contents']:
                    path = obj['Key']
                    key = self._path_to_key(path)
                    yield key, self, S3Meta(path=path, location=self)

    @contextmanager
    def read(
        self, key: Key, return_labels: bool
    ) -> ContextManager[Union[None, Value, Tuple[Value, MaybeLabels]]]:
        try:
            path = self._key_to_path(key)
            try:
                self.update_usage_date(path)
                if return_labels:
                    with self._get_buffer(path) as buffer:
                        yield buffer, self.get_labels(path)
                else:
                    with self._get_buffer(path) as buffer:
                        yield buffer

            except ClientError as e:
                if (
                    e.response['ResponseMetadata']['HTTPStatusCode'] == 404
                    or e.response['Error']['Code'] == 'NoSuchKey'
                ):  # file doesn't exist
                    yield
                else:
                    raise
            except ConnectionError:
                yield
        except StorageCorruption:
            self.delete(key)

    @contextmanager
    def write(self, key: Key, value: Value, labels: MaybeLabels) -> ContextManager:
        try:
            path = self._key_to_path(key)
            with value_to_buffer(value) as value:
                try:
                    with self._get_buffer(path) as obj_body_buffer:
                        try:
                            match_buffers(value, obj_body_buffer, context=key.hex())
                        except ValueError as e:
                            raise CollisionError(
                                f'Written value and the new one does not match: {key}'
                            ) from e
                        self.update_labels(path, labels)
                        self.update_usage_date(path)
                        with self._get_buffer(path) as buffer:
                            yield buffer
                            return
                except ClientError as e:
                    if (
                        e.response['ResponseMetadata']['HTTPStatusCode'] == 404
                        or e.response['Error']['Code'] == 'NoSuchKey'
                    ):
                        self.s3.upload_fileobj(
                            Bucket=self.bucket, Key=path, Fileobj=value
                        )
                        self.update_labels(path, labels)
                        self.update_usage_date(path)
                        with self._get_buffer(path) as buffer:
                            yield buffer
                            return
                    else:
                        raise
                except ConnectionError:
                    yield
        except StorageCorruption:
            self.delete(key)

    def delete(self, key: Key):
        path = self._key_to_path(key)
        self.s3.delete_object(Bucket=self.bucket, Key=path)
        return True

    def update_labels(self, path: str, labels: MaybeLabels):
        if labels is not None:
            tags_dict = self._tags_to_dict(
                self.s3.get_object_tagging(Bucket=self.bucket, Key=path)['TagSet']
            )
            tags_dict.update({f'_{label}': f'_{label}' for label in labels})
            tags = self._dict_to_tags(tags_dict)
            self.s3.put_object_tagging(
                Bucket=self.bucket, Key=path, Tagging={'TagSet': tags}
            )

    def get_labels(self, path: str) -> MaybeLabels:
        tags_dict = self._tags_to_dict(
            self.s3.get_object_tagging(Bucket=self.bucket, Key=path)['TagSet']
        )
        return [dict_key[1:] for dict_key in tags_dict if dict_key.startswith('_')]

    def update_usage_date(self, path: str):
        try:
            tags_dict = self._tags_to_dict(
                self.s3.get_object_tagging(Bucket=self.bucket, Key=path)['TagSet']
            )
            tags_dict['usage_date'] = str(datetime.now().timestamp())
            tags = self._dict_to_tags(tags_dict)
            self.s3.put_object_tagging(
                Bucket=self.bucket, Key=path, Tagging={'TagSet': tags}
            )
        except KeyError:
            warnings.warn(f'Cannot update usage date for the key {self._path_to_key(path)}', stacklevel=2)

    def get_usage_date(self, path: str) -> Optional[datetime]:
        tags_dict = self._tags_to_dict(
            self.s3.get_object_tagging(Bucket=self.bucket, Key=path)['TagSet']
        )
        if 'usage_date' in tags_dict:
            return datetime.fromtimestamp(float(tags_dict['usage_date']))
        return None

    def _get_buffer(self, path):
        # with self.s3.get_object(Bucket=self.bucket, Key=path)['Body'] as obj_body:
        #     assert False, obj_body.read()
        return StreamingBodyBuffer(self.s3.get_object, Bucket=self.bucket, Key=path)

    def _key_to_path(self, key: Key):
        return str(key_to_relative(key, [2, -1]))

    def _path_to_key(self, path: str):
        return bytes.fromhex(path.replace('/', ''))

    @staticmethod
    def _tags_to_dict(tags: Iterable[Mapping[str, str]]) -> Mapping[str, str]:
        return {tag['Key']: tag['Value'] for tag in tags}

    @staticmethod
    def _dict_to_tags(tag_dict: Mapping[str, str]) -> Iterable[Mapping[str, str]]:
        return [{'Key': key, 'Value': value} for key, value in tag_dict.items()]


class StreamingBodyBuffer(BinaryIO):
    def __init__(self, getter, **kwargs):
        super().__init__()
        self.getter, self.kwargs = getter, kwargs
        self._streaming_body = getter(**kwargs).get('Body')

    def seek(self, offset: int, whence: int = SEEK_SET) -> int:
        # we can either return to the begining of the stream or do nothing
        #  everythnig else is too expensive
        if whence == SEEK_SET:
            if offset == 0:
                self._streaming_body = self.getter(**self.kwargs).get('Body')
                return 0
            if offset == self.tell():
                return offset

        if whence == SEEK_CUR:
            if offset == 0:
                return self.tell()
            if offset == -self.tell():
                self._streaming_body = self.getter(**self.kwargs).get('Body')
                return 0

        raise NotImplementedError('Cannot seek anywhere but the begining of the stream')

    def seekable(self):
        return True

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def __getattribute__(self, attr) -> Any:
        if attr in ('seek', 'getter', 'kwargs', '__enter__', '__exit__'):
            return super().__getattribute__(attr)
        streaming_body = super().__getattribute__('_streaming_body')
        return getattr(streaming_body, attr)


class S3Meta(Meta):
    def __init__(self, path, location):
        self._path, self._location = path, location

    @property
    def last_used(self) -> Optional[datetime]:
        return self._location.get_usage_date(self._path)

    @property
    def labels(self) -> MaybeLabels:
        return self._location.get_labels(self._path)

    def __str__(self):
        return f'{self.last_used}, {self.labels}'
