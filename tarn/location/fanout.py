from contextlib import contextmanager
from typing import ContextManager, Iterable, Tuple, Union

from ..compat import Self
from ..interface import Key, Keys, MaybeLabels, MaybeValue, Meta, Value
from ..utils import is_binary_io
from .interface import Location, Writable


class Fanout(Writable):
    def __init__(self, *locations: Location):
        sizes = {location.key_size for location in locations if location.key_size is not None}
        hashes = {location.hash for location in locations if location.hash is not None}
        assert len(sizes) <= 1, sizes
        assert len(hashes) <= 1, hashes

        self._locations = locations
        self.key_size = sizes.pop() if sizes else None
        self.hash = hashes.pop() if hashes else None

    @contextmanager
    def read(self, key: Key, return_labels: bool) -> ContextManager[Union[None, Value, Tuple[Value, MaybeLabels]]]:
        for location in self._locations:
            leave = False
            with location.read(key, return_labels) as value:
                if value is not None:
                    leave = True
                    yield value

            # see more info on the "leave" trick in `Levels`
            if leave:
                return

        yield None

    @contextmanager
    def write(self, key: Key, value: Value, labels: MaybeLabels) -> ContextManager[MaybeValue]:
        for location in self._locations:
            if isinstance(location, Writable):
                if is_binary_io(value):
                    offset = value.tell()
                leave = False
                with location.write(key, value, labels) as written:
                    if written is not None:
                        leave = True
                        yield written
                # see more info on the "leave" trick in `Levels`
                if leave:
                    return
                if is_binary_io(value) and offset != value.tell():
                    value.seek(offset)
        yield None

    def delete(self, key: Key) -> bool:
        deleted = False
        for location in self._locations:
            if isinstance(location, Writable):
                if location.delete(key):
                    deleted = True

        return deleted

    def read_batch(self, keys: Keys) -> Iterable[Tuple[Key, MaybeValue]]:
        for location in self._locations:
            if not keys:
                return

            remaining = []
            for key, value in location.read_batch(keys):
                if value is None:
                    remaining.append(key)
                else:
                    yield key, value

            keys = remaining

        for key in keys:
            yield key, None

    def contents(self) -> Iterable[Tuple[Key, Self, Meta]]:
        for location in self._locations:
            yield from location.contents()
