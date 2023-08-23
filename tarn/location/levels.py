import sys
from contextlib import contextmanager
from itertools import islice
from typing import ContextManager, Iterable, NamedTuple, Optional, Tuple, Union

from ..compat import Self
from ..interface import Key, Keys, MaybeLabels, MaybeValue, Meta, Value
from ..location import Location, Writable
from ..utils import is_binary_io
from .fanout import _get_not_none


class Level(NamedTuple):
    location: Location
    write: bool = True
    replicate: bool = True
    name: Optional[str] = None


class Levels(Writable):
    def __init__(self, *levels: Union[Level, Location]):
        levels = [
            level if isinstance(level, Level) else Level(level, write=True, replicate=True)
            for level in levels
        ]
        hashes = _get_not_none((level.location for level in levels), 'hash')
        assert len(hashes) <= 1, hashes

        self._levels = levels
        self.hash = hashes.pop() if hashes else None

    @contextmanager
    def read(self, key: Key, return_labels: bool) -> ContextManager[Union[None, Value, Tuple[Value, MaybeLabels]]]:
        for index, config in enumerate(self._levels):
            leave = False
            with config.location.read(key, True) as value:
                if value is not None:
                    # we must leave the loop after the first successful read
                    leave = True
                    # try to write to a level with higher priority
                    with self._replicate(key, *value, index) as (value_copy, labels_copy):
                        if return_labels:
                            yield value_copy, labels_copy
                        else:
                            yield value_copy

            # but the context manager might have silenced the error, so we need an extra return here
            if leave:
                return

        yield None

    @contextmanager
    def write(self, key: Key, value: Value, labels: MaybeLabels) -> ContextManager[MaybeValue]:
        for config in self._levels:
            location = config.location
            if config.write and isinstance(location, Writable):
                if is_binary_io(value):
                    offset = value.tell()
                leave = False
                with location.write(key, value, labels) as written:
                    if written is not None:
                        # we must leave the loop after the first successful write
                        leave = True
                        yield written
                # but the context manager might have silenced the error, so we need an extra return here
                if leave:
                    return
                if is_binary_io(value) and offset != value.tell():
                    value.seek(offset)

        yield None

    def delete(self, key: Key) -> bool:
        deleted = False
        for config in self._levels:
            if config.write and isinstance(config.location, Writable):
                if config.location.delete(key):
                    deleted = True

        return deleted

    def read_batch(self, keys: Keys) -> Iterable[Tuple[Key, Tuple[Value, MaybeLabels]]]:
        for index, config in enumerate(self._levels):
            if not keys:
                return

            remaining = []
            for key, value in config.location.read_batch(keys):
                if value is None:
                    remaining.append(key)
                else:
                    with self._replicate(key, *value, index) as mirrored:
                        yield key, mirrored

            keys = remaining

        for key in keys:
            yield key, None

    def contents(self) -> Iterable[Tuple[Key, Self, Meta]]:
        for level in self._levels:
            yield from level.location.contents()

    @contextmanager
    def _replicate(self, key: Key, value: Value, labels: MaybeLabels, index: int):
        for config in islice(self._levels, index):
            location = config.location
            if config.replicate and isinstance(location, Writable):
                if is_binary_io(value):
                    offset = value.tell()
                with _propagate_exception(location.write(key, value, labels)) as written:
                    if written is not None:
                        yield written, labels
                        return
                if is_binary_io(value) and offset != value.tell():
                    value.seek(offset)

        yield value, labels


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
