from contextlib import contextmanager
from typing import ContextManager, Iterable, Tuple, Union

from ..compat import Self
from ..interface import Key, Keys, MaybeLabels, Value
from ..utils import value_to_buffer
from .interface import Meta, Writable


class SmallLocation(Writable):
    def __init__(self, location: Writable, max_size: int):
        self.location = location
        self.max_size = max_size
        self.hash = None
        self.key_size = None

    def contents(self) -> Iterable[Tuple[Key, Self, Meta]]:
        yield from self.location.contents()

    @contextmanager
    def read(self, key: Key, return_labels: bool) -> ContextManager:
        with self.location.read(key, return_labels) as buffer:
            yield buffer

    def read_batch(self, keys: Keys) -> Iterable[Tuple[Key, Union[None, Tuple[Value, MaybeLabels]]]]:
        yield from self.location.read_batch(keys)

    @contextmanager
    def write(self, key: Key, value: Value, labels: MaybeLabels) -> ContextManager:
        with value_to_buffer(value) as value:
            content = value.read(self.max_size + 1)
            if len(content) < self.max_size:
                with self.location.write(key, content, labels) as buffer:
                    yield buffer
                    return
            yield

    def delete(self, key: Key) -> bool:
        return self.location.delete(key)
