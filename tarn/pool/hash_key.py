import os
from contextlib import contextmanager
from typing import Iterable, Tuple, Callable, Any, ContextManager, Optional, Union, Type, Sequence

from ..compat import HashAlgorithm
from ..digest import digest_value
from ..exceptions import WriteError, ReadError
from ..interface import Keys, Key, MaybeValue, PathOrStr
from ..location import Location, DiskDict, Fanout, Levels

LocationLike = Union[Location, PathOrStr]
LocationsLike = Union[LocationLike, Sequence[LocationLike]]


class HashKeyStorage:
    def __init__(self, local: LocationsLike, remote: LocationsLike = (), fetch: bool = True, error: bool = True,
                 algorithm: Type[HashAlgorithm] = None):
        local = resolve_location(local)
        remote = resolve_location(remote)
        hashes = {location.hash for location in (local, remote) if location.hash is not None}
        assert len(hashes) <= 1
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
        self.algorithm = algorithm
        self.digest_size = algorithm().digest_size

    def fetch(self, keys: Keys) -> Iterable[Tuple[Key, bool]]:
        for key, value in self._full.read_batch(keys):
            yield key, value is not None

    def read(self, func_or_key, *args, **kwargs):
        if callable(func_or_key):
            return self._read_func(func_or_key, *args, **kwargs)
        return self._read_context(func_or_key, *args, **kwargs)

    @contextmanager
    def _read_context(self, key: Key, fetch: Optional[bool] = None,
                      error: Optional[bool] = None) -> ContextManager[MaybeValue]:
        # TODO: resolve args
        if isinstance(key, str):
            key = bytes.fromhex(key)
        location = self._full if fetch else self._local
        with location.read(key) as value:
            if value is None and error:
                raise ReadError(f'The key {key.hex()} is not found')
            yield value

    def _read_func(self, func: Callable, key: Key, *args, fetch: Optional[bool] = None, error: Optional[bool] = None,
                   **kwargs) -> Any:
        with self._read_context(key, fetch, error) as value:
            return func(value, *args, **kwargs)

    # TODO: add support for buffers
    def write(self, value: PathOrStr, error: bool = True) -> Optional[Key]:
        digest = digest_value(value, self.algorithm)
        with self._local.write(digest, value) as written:
            # TODO: check digest?
            if written is None:
                if error:
                    raise WriteError(digest.hex())
                return None

        return digest


def resolve_location(location):
    if isinstance(location, (os.PathLike, str)):
        location = DiskDict(location)
    if isinstance(location, Location):
        return location

    if isinstance(location, (list, tuple)):
        return Fanout(*list(map(resolve_location, location)))

    raise TypeError(f'Unsupported location type: {location}')
