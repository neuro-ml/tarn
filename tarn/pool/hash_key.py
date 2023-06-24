import os
from contextlib import contextmanager
from io import BytesIO
from pathlib import Path
from typing import Any, Callable, ContextManager, Iterable, Optional, Sequence, Tuple, Type, Union

from ..compat import HashAlgorithm
from ..digest import digest_value
from ..exceptions import ReadError, WriteError
from ..interface import Key, Keys, MaybeLabels, MaybeValue, PathOrStr, Value
from ..location import DiskDict, Fanout, Levels, Location

LocationLike = Union[Location, PathOrStr]
LocationsLike = Union[LocationLike, Sequence[LocationLike]]


class HashKeyStorage:
    def __init__(self, local: LocationsLike, remote: LocationsLike = (), fetch: bool = True, error: bool = True,
                 algorithm: Optional[Type[HashAlgorithm]] = None, labels: MaybeLabels = None):
        local = resolve_location(local)
        remote = resolve_location(remote)
        hashes = {location.hash for location in (local, remote) if location.hash is not None}
        assert len(hashes) <= 1, hashes
        if algorithm is None:
            assert hashes
            algorithm, = hashes
        elif hashes:
            assert algorithm == hashes.pop()

        self._local = local
        self._remote = remote
        self._full = Levels(local, remote)
        self._error = error
        self._fetch = fetch
        self.labels = labels
        self.algorithm = algorithm
        self.digest_size = algorithm().digest_size

    def fetch(self, keys: Keys) -> Iterable[Tuple[Key, bool]]:
        for key, value in self._full.read_batch(keys):
            yield key, value is not None

    def read(self, func_or_key, *args, fetch: Optional[bool] = None, error: Optional[bool] = None, **kwargs):
        if callable(func_or_key):
            return self._read_func(func_or_key, *args, error=error, fetch=fetch, **kwargs)
        return self._read_context(func_or_key, *args, **kwargs)

    @contextmanager
    def _read_context(self, key, fetch, error) -> ContextManager[MaybeValue]:
        fetch = self._resolve_value(fetch, self._fetch, 'fetch')
        error = self._resolve_value(error, self._error, 'error')

        if isinstance(key, str):
            key = bytes.fromhex(key)
        location = self._full if fetch else self._local
        with location.read(key, False) as value:
            if value is None and error:
                raise ReadError(f'The key {key.hex()} is not found')
            yield value

    def _read_func(self, func: Callable, key: Key, *args, fetch, error, **kwargs) -> Any:
        with self._read_context(key, fetch, error) as value:
            return func(value, *args, **kwargs)

    def write(self, value: Union[Value, bytes], error: Optional[bool] = None,
              labels: MaybeLabels = None) -> Optional[Key]:
        error = self._resolve_value(error, self._error, 'error')
        labels = self._resolve_value(labels, self.labels, None)

        if isinstance(value, (bytes, Path, str)):
            digest = digest_value(value, self.algorithm)
        else:
            assert value.seekable(), value
            position = value.tell()
            digest = digest_value(value, self.algorithm)
            value.seek(position)

        if isinstance(value, bytes):
            value = BytesIO(value)

        with self._local.write(digest, value, labels) as written:
            # TODO: check digest?
            if written is None:
                if error:
                    raise WriteError(digest.hex())
                return None

        return digest

    @staticmethod
    def _resolve_value(current, preset, name):
        if current is None:
            current = preset
        if current is None and name is not None:
            raise ValueError(f'Must provide a value for the "{name}" argument')
        return current


def resolve_location(location):
    if isinstance(location, (os.PathLike, str)):
        location = DiskDict(location)
    if isinstance(location, Location):
        return location

    if isinstance(location, (list, tuple)):
        return Fanout(*list(map(resolve_location, location)))

    raise TypeError(f'Unsupported location type: {location}')
