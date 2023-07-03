import json
from contextlib import contextmanager
from typing import Any, ContextManager, Iterable, Optional, Tuple, Union

from redis import Redis

from ..digest import value_to_buffer
from ..interface import Key, Keys, MaybeLabels, Meta, Value
from .interface import Writable


class RedisLocation(Writable):
    def __init__(self, redis: Union[Redis, str], prefix: str = ''):
        if isinstance(redis, str):
            redis = Redis.from_url(redis)
        self.redis = redis
        self.prefix = prefix
        self.hash = None
        self.key_size = None

    def contents(self) -> Iterable[Tuple[Key, Any, Meta]]:
        for key in self.redis.scan_iter(match=f'{self.prefix}*'):
            yield key[len(self.prefix):], self, RedisMeta(key=key, location=self)

    @contextmanager
    def read(self, key: Key, return_labels: bool) -> ContextManager:
        content_key = self.prefix + key.hex()
        content = self.redis.get(content_key)
        if content is None:
            yield
            return
        if return_labels:
            labels = self.get_labels(key)
            yield value_to_buffer(content), labels
            return
        yield value_to_buffer(content)
    
    def read_batch(self, keys: Keys) -> Iterable[Optional[Tuple[Key, Tuple[Value, MaybeLabels]]]]:
        for key in keys:
            with self.read(key, True) as value:
                yield key, value
                
    @contextmanager
    def write(self, key: Key, value: Value, labels: MaybeLabels) -> ContextManager:
        content_key = self.prefix + key.hex()
        with value_to_buffer(value) as value:
            content = self.redis.get(content_key)
            if content is None:
                self.redis.set(content_key, value.read())
                self.set_labels(key, labels)
                yield value_to_buffer(self.redis.get(content_key))
                return
            old_content = self.redis.get(content_key)
            if old_content != value.read():
                raise ValueError(f"Written value and the new one doesn't match: {key}")
            self.set_labels(key, labels)
            yield value_to_buffer(self.redis.get(content_key))

    def get_labels(self, key: Key) -> MaybeLabels:
        labels_key = f'labels{self.prefix}{key.hex()}'
        labels_bytes = self.redis.get(labels_key)
        if labels_bytes is None:
            return
        return list(json.loads(labels_bytes))

    def set_labels(self, key: Key, labels: MaybeLabels):
        labels_key = f'labels{self.prefix}{key.hex()}'
        old_labels = self.get_labels(key) or []
        if labels is not None:
            labels = list(set(old_labels).union(labels))
            self.redis.set(labels_key, json.dumps(labels))

    def delete(self, key: Key):
        content_key = self.prefix + key.hex()
        labels_key = f'labels{self.prefix}{key.hex()}'
        self.redis.delete(content_key, labels_key)


class RedisMeta(Meta):
    def __init__(self, key, location):
        self._key, self._location = key, location

    @property
    def labels(self) -> MaybeLabels:
        return self._location.get_labels(self._key)

    def __str__(self):
        return self.labels
