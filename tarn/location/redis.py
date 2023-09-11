import json
from contextlib import contextmanager
from datetime import datetime
from typing import Any, AnyStr, ContextManager, Iterable, Optional, Tuple

from redis import Redis

from ..digest import value_to_buffer
from ..exceptions import CollisionError, StorageCorruption
from ..interface import Key, MaybeLabels, Meta, Value
from .interface import Writable


class RedisLocation(Writable):
    def __init__(self, *redis_or_args, prefix: AnyStr = '', **kwargs):
        if len(redis_or_args) == 1 and isinstance(redis_or_args[0], Redis):
            assert not kwargs, kwargs
            redis, = redis_or_args
        else:
            redis = Redis(*redis_or_args, **kwargs)
        if isinstance(prefix, str):
            prefix = prefix.encode()
        self.redis = redis
        self.prefix = prefix

    def contents(self) -> Iterable[Tuple[Key, Any, Meta]]:
        for raw_key in self.redis.scan_iter(match=self.prefix + b'*'):
            key = raw_key[len(self.prefix):]
            yield key, self, RedisMeta(key=key, location=self)

    @contextmanager
    def read(self, key: Key, return_labels: bool) -> ContextManager:
        try:
            content_key = self.prefix + key
            content = self.redis.get(content_key)
            if content is None:
                yield
                return
            self.update_usage_date(key)
            if return_labels:
                labels = self.get_labels(key)
                with value_to_buffer(self.redis.get(content_key)) as buffer:
                    yield buffer, labels
                    return
            with value_to_buffer(self.redis.get(content_key)) as buffer:
                yield buffer
        except StorageCorruption:
            self.delete(key)

    @contextmanager
    def write(self, key: Key, value: Value, labels: MaybeLabels) -> ContextManager:
        try:
            content_key = self.prefix + key
            with value_to_buffer(value) as value:
                content = self.redis.get(content_key)
                if content is None:
                    self.redis.set(content_key, value.read())
                    self.update_labels(key, labels)
                    self.update_usage_date(key)
                    with value_to_buffer(self.redis.get(content_key)) as buffer:
                        yield buffer
                        return
                old_content = self.redis.get(content_key)
                if old_content != value.read():
                    raise CollisionError(
                        f'Written value and the new one does not match: {key}'
                    )
                self.update_labels(key, labels)
                self.update_usage_date(key)
                with value_to_buffer(self.redis.get(content_key)) as buffer:
                    yield buffer
        except StorageCorruption:
            self.delete(key)

    def get_labels(self, key: Key) -> MaybeLabels:
        labels_key = b'labels' + self.prefix + key
        labels_bytes = self.redis.get(labels_key)
        if labels_bytes is None:
            return
        return list(json.loads(labels_bytes))

    def update_labels(self, key: Key, labels: MaybeLabels):
        labels_key = b'labels' + self.prefix + key
        old_labels = self.get_labels(key) or []
        if labels is not None:
            labels = list(set(old_labels).union(labels))
            self.redis.set(labels_key, json.dumps(labels))

    def get_usage_date(self, key: Key) -> Optional[datetime]:
        usage_date_key = b'usage_date' + self.prefix + key
        usage_date = self.redis.get(usage_date_key)
        if usage_date is not None:
            return datetime.fromtimestamp(float(usage_date))

    def update_usage_date(self, key: Key):
        usage_date_key = b'usage_date' + self.prefix + key
        self.redis.set(usage_date_key, datetime.now().timestamp())

    def delete(self, key: Key):
        content_key = self.prefix + key
        labels_key = b'labels' + self.prefix + key
        usage_date_key = b'usage_date' + self.prefix + key
        self.redis.delete(content_key, labels_key, usage_date_key)
        return True

    @classmethod
    def _from_args(cls, prefix, kwargs):
        return cls(prefix=prefix, **kwargs)

    def __reduce__(self):
        return self._from_args, (self.prefix, self.redis.get_connection_kwargs())

    def __eq__(self, other):
        return isinstance(other, RedisLocation) and self.__reduce__() == other.__reduce__()


class RedisMeta(Meta):
    def __init__(self, key, location):
        self._key, self._location = key, location

    @property
    def last_used(self) -> Optional[datetime]:
        return self._location.get_usage_date(self._key)

    @property
    def labels(self) -> MaybeLabels:
        return self._location.get_labels(self._key)

    def __str__(self):
        return f'{self.last_used}, {self.labels}'
