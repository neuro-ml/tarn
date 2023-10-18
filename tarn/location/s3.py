import warnings
from contextlib import contextmanager
from datetime import datetime
from pickle import PicklingError
from typing import Any, ContextManager, Iterable, Optional, Tuple, Union

from s3fs.core import S3FileSystem

from ..digest import key_to_relative, value_to_buffer
from ..exceptions import CollisionError, StorageCorruption
from ..interface import Key, MaybeLabels, Meta, Value
from ..utils import match_buffers
from .interface import Location


class S3(Location):
    def __init__(self, s3fs_or_url: Optional[Union[S3FileSystem, str]], bucket_name: str, **kwargs):
        self.bucket = bucket_name
        if s3fs_or_url is None:
            self.s3 = S3FileSystem(**kwargs)
        elif isinstance(s3fs_or_url, str):
            self.s3 = S3FileSystem(client_kwargs={'endpoint_url': s3fs_or_url}, **kwargs)
        else:
            assert isinstance(s3fs_or_url, S3FileSystem), 's3fs_or_url should be either None, or str, or S3FileSystem'
            self.s3 = s3fs_or_url
        self._s3fs_or_url = s3fs_or_url
        self._kwargs = kwargs

    def contents(self) -> Iterable[Tuple[Key, Any, Meta]]:
        for directory, _, files in self.s3.walk(self.bucket):
            for file in files:
                path = f'{directory}/{file}'
                key = self._path_to_key(path)
                yield key, self, S3Meta(path=path, location=self)

    @contextmanager
    def read(
            self, key: Key, return_labels: bool
    ) -> ContextManager[Union[None, Value, Tuple[Value, MaybeLabels]]]:
        try:
            path = self._key_to_path(key)
            try:
                self.touch(key)
                if return_labels:
                    with self.s3.open(path, 'rb') as buffer:
                        yield buffer, self._get_labels(path)
                else:
                    with self.s3.open(path, 'rb') as buffer:
                        yield buffer

            except FileNotFoundError:
                yield
        except StorageCorruption:
            self.delete(key)

    @contextmanager
    def write(self, key: Key, value: Value, labels: MaybeLabels) -> ContextManager:
        try:
            path = self._key_to_path(key)
            with value_to_buffer(value) as value:
                try:
                    with self.s3.open(path, 'rb') as buffer:
                        try:
                            match_buffers(value, buffer, context=key.hex())
                        except ValueError as e:
                            raise CollisionError(
                                f'Written value and the new one does not match: {key}'
                            ) from e
                        self._update_labels(path, labels)
                        self.touch(key)
                        with self.s3.open(path, 'rb') as buffer:
                            yield buffer
                            return
                except FileNotFoundError:
                    with self.s3.open(path, 'wb') as buffer:
                        buffer.write(value.read())
                    self._update_labels(path, labels)
                    self.touch(key)
                    with self.s3.open(path, 'rb') as buffer:
                        yield buffer
                        return
        except StorageCorruption:
            self.delete(key)

    def delete(self, key: Key):
        path = self._key_to_path(key)
        self.s3.delete(path)
        return True

    def touch(self, key: Key):
        try:
            path = self._key_to_path(key)
            tags_dict = self.s3.get_tags(path)
            tags_dict['usage_date'] = str(datetime.now().timestamp())
            self.s3.put_tags(path, tags_dict)
            return True
        except FileNotFoundError:
            return False

    def _update_labels(self, path: str, labels: MaybeLabels):
        if labels is not None:
            tags_dict = self.s3.get_tags(path)
            tags_dict.update({f'_{label}': f'_{label}' for label in labels})
            self.s3.put_tags(path, tags_dict)

    def _get_labels(self, path: str) -> MaybeLabels:
        tags_dict = self.s3.get_tags(path)
        return [dict_key[1:] for dict_key in tags_dict if dict_key.startswith('_')]

    def _get_usage_date(self, path: str) -> Optional[datetime]:
        tags_dict = self.s3.get_tags(path)
        if 'usage_date' in tags_dict:
            return datetime.fromtimestamp(float(tags_dict['usage_date']))
        return None

    def _key_to_path(self, key: Key):
        return f'{self.bucket}/{str(key_to_relative(key, [2, -1]))}'

    def _path_to_key(self, path: str):
        path = ''.join(path.split('/')[1:])
        try:
            return bytes.fromhex(path)
        except ValueError:
            assert False, path

    @classmethod
    def _from_args(cls, s3fs_or_url, bucket_name, kwargs):
        return cls(s3fs_or_url, bucket_name, **kwargs)

    def __reduce__(self):
        if isinstance(self._s3fs_or_url, (str, None)):
            return self._from_args, (self._s3fs_or_url, self.bucket, self._kwargs)
        raise PicklingError('Cannot pickle S3Client')

    def __eq__(self, other):
        return isinstance(other, S3) and self.__reduce__() == other.__reduce__()


class S3Meta(Meta):
    def __init__(self, path: str, location: S3):
        self._path, self._location = path, location

    @property
    def last_used(self) -> Optional[datetime]:
        return self._location._get_usage_date(self._path)

    @property
    def labels(self) -> MaybeLabels:
        return self._location._get_labels(self._path)

    def __str__(self):
        return f'{self.last_used}, {self.labels}'
