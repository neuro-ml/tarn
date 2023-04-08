from contextlib import contextmanager
from typing import ContextManager, Iterable, Tuple

from ..interface import Key, Keys, MaybeValue, Value
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
    def read(self, key: Key) -> ContextManager[MaybeValue]:
        raised = False
        for location in self._locations:
            with location.read(key) as value:
                if value is not None:
                    try:
                        yield value
                        return
                    except BaseException:
                        raised = True
                        raise

            if raised:
                return

        yield None

    @contextmanager
    def write(self, key: Key, value: Value) -> ContextManager[MaybeValue]:
        raised = False
        for location in self._locations:
            if isinstance(location, Writable):
                with location.write(key, value) as written:
                    if written is not None:
                        try:
                            yield written
                            return
                        except BaseException:
                            raised = True
                            raise

                if raised:
                    return

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
