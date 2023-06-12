from contextlib import contextmanager
from pathlib import Path
from typing import ContextManager, Iterable, Tuple

from botocore.exceptions import ClientError
from mypy_boto3_s3 import ServiceResource

from .interface import Writable
from ..config import load_config
from ..digest import key_to_relative
from ..interface import Key, Keys, MaybeLabels, MaybeValue, PathOrStr, Value
from ..tools.labels import LabelsStorage


class S3(Writable):
    def __init__(self, s3_resource: ServiceResource, bucket_name: str, root: PathOrStr):
        root = Path(root)
        config = load_config(root)
        self.levels = config.levels
        self.hash = config.hash.build()
        self.bucket = bucket_name
        self.s3 = s3_resource
        labels_folder = self.root / 'tools/labels'
        self.labels: LabelsStorage = config.make_labels(labels_folder)

    @contextmanager
    def read(self, key: Key) -> ContextManager[MaybeValue]:
        file = self._key_to_path(key)
        try:
            s3_object = self.s3.Object(self.bucket, file)
            s3_response = s3_object.get()
            s3_object_body = s3_response.get('Body')
            yield s3_object_body.read()
        except ClientError as e:
            if e.response['Error']['Code'] == "404": # file doesn't exist
                yield
                return
            else:
                raise

    def read_batch(self, keys: Keys) -> Iterable[Tuple[Key, MaybeValue]]:
        for key in keys:
            yield key, self.read(key)

    def write(self, key: Key, value: Value, labels: MaybeLabels) -> ContextManager:
        file = self._key_to_path(key)
        try:
            self.s3.Object(self.bucket, file).load()
            self.labels.update(key, labels)
            yield file
            return
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                s3_object = self.s3.Object(self.bucket, file)
                s3_object.put(Body=value)
                self.labels.update(key, labels)
                yield file
                return
            else:
                raise

    def delete(self, key: Key):
        file = self._key_to_path(key)
        self.s3.Object(self.bucket, file)

    def _key_to_path(self, key: Key):
        return self.root / key_to_relative(key, self.levels)
