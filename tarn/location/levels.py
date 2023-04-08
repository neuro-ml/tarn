import sys
from contextlib import contextmanager
from itertools import islice
from typing import ContextManager, Iterable, NamedTuple, Optional, Tuple, Union

from ..interface import Key, Keys, MaybeValue, Value
from ..location import Location, Writable


class Level(NamedTuple):
    location: Location
    write: bool = True
    replicate: bool = True
    name: Optional[str] = None


class Levels(Writable):
    def __init__(self, *levels: Union[Level, Location]):
        levels = [
            Level(level, write=True, replicate=True) if isinstance(level, Location) else level
            for level in levels
        ]
        sizes = {level.location.key_size for level in levels if level.location.key_size is not None}
        hashes = {level.location.hash for level in levels if level.location.hash is not None}
        assert len(sizes) <= 1, sizes
        assert len(hashes) <= 1, hashes

        self._levels = levels
        self.key_size = sizes.pop() if sizes else None
        self.hash = hashes.pop() if hashes else None

    @contextmanager
    def read(self, key: Key) -> ContextManager[MaybeValue]:
        # TODO: ExitStack?
        #  https://docs.python.org/3/library/contextlib.html#replacing-any-use-of-try-finally-and-flag-variables
        raised = False
        for index, config in enumerate(self._levels):
            with config.location.read(key) as value:
                if value is not None:
                    # try to write to a level with higher priority
                    with self._replicate(key, value, index) as mirrored:
                        try:
                            yield mirrored
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
        for config in self._levels:
            location = config.location
            if config.write and isinstance(location, Writable):
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
        for config in self._levels:
            if config.write and isinstance(config.location, Writable):
                if config.location.delete(key):
                    deleted = True

        return deleted

    def read_batch(self, keys: Keys) -> Iterable[Tuple[Key, MaybeValue]]:
        for index, config in enumerate(self._levels):
            if not keys:
                return

            remaining = []
            for key, value in config.location.read_batch(keys):
                if value is None:
                    remaining.append(key)
                else:
                    with self._replicate(key, value, index) as mirrored:
                        yield key, mirrored

            keys = remaining

        for key in keys:
            yield key, None

    @contextmanager
    def _replicate(self, key: Key, value: Value, index: int):
        for config in islice(self._levels, index):
            location = config.location
            if config.replicate and isinstance(location, Writable):
                with _propagate_exception(location.write(key, value)) as written:
                    if written is not None:
                        yield written
                        return

        yield value


@contextmanager
def _propagate_exception(cm: ContextManager):
    value = cm.__enter__()
    clean = True
    try:
        try:
            yield value
        except BaseException:
            clean = False
            # the current context manager either:
            # 1. exits gracefully - we propagate the current exception
            # 2. raises a new exception - we just propagate the new exception
            cm.__exit__(*sys.exc_info())
            raise
    finally:
        # no exception was raised - exit gracefully
        if clean:
            cm.__exit__(None, None, None)
