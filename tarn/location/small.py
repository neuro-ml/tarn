from contextlib import contextmanager
from io import BytesIO
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

    def contents(self) -> Iterable[Tuple[Key, Self, Meta]]:
        return self.location.contents()

    def read(self, key: Key, return_labels: bool) -> ContextManager:
        return self.location.read(key, return_labels)

    def read_batch(self, keys: Keys) -> Iterable[Tuple[Key, Union[None, Tuple[Value, MaybeLabels]]]]:
        return self.location.read_batch(keys)

    @contextmanager
    def write(self, key: Key, value: Value, labels: MaybeLabels) -> ContextManager:
        with value_to_buffer(value) as value:
            content = read_at_least(value, self.max_size + 1)
        if not content or len(content) > self.max_size:
            yield
            return

        with self.location.write(key, BytesIO(content), labels) as buffer:
            yield buffer

    def delete(self, key: Key) -> bool:
        return self.location.delete(key)


# `read` is only guaranteed to return _at most_ n bytes, so we might need several calls
def read_at_least(buffer, n):
    result = b''
    while len(result) < n:
        value = buffer.read(n)
        if not value:
            break
        result += value
    return result
