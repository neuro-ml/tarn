import logging
from pathlib import Path
from typing import Sequence, Iterable, Callable, Union

from ..digest import digest_file
from ..exceptions import ReadError
from ..interface import StorageBase, Key, RemoteStorage, WriteError, StorageLevel
from ..utils import PathLike
from .disk import Disk

logger = logging.getLogger(__name__)


class Storage:
    def __init__(self, *local: Union[StorageLevel, Sequence[Disk], Disk, Sequence[PathLike], PathLike],
                 remote: Sequence[RemoteStorage] = ()):
        normalized = tuple(normalize_levels(local, Disk))

        self.storage = StorageBase(*normalized, remote=remote)
        self.algorithm = self.storage.hash.build()
        self.digest_size = sum(normalized[0].locations[0].levels)

    @property
    def levels(self) -> Sequence[StorageLevel]:
        return self.storage.levels

    def read(self, func: Callable, key: Key, *args, fetch: bool = True, error: bool = True, **kwargs):
        value, success = self.storage.read(key, lambda x: func(x, *args, **kwargs), fetch=fetch)
        if not error:
            return value, bool(success)

        if success:
            return value
        if success is None:
            raise WriteError(f"The key {key} couldn't be written to any storage")
        if fetch:
            message = f'Key {key} is not present neither locally nor among your {len(self.storage.remote)} remotes'
        else:
            message = f'Key {key} is not present locally'
        raise ReadError(message)

    def write(self, file: PathLike, error: bool = True) -> Union[Key, None]:
        file = Path(file)
        assert file.exists(), file
        key = digest_file(file, self.algorithm)
        if not self.storage.write(key, file, None):
            if error:
                raise WriteError('The file could not be written to any storage')
            return None

        return key

    def fetch(self, keys: Iterable[Key], *, verbose: bool) -> Sequence[Key]:
        return self.storage.fetch(keys, None, verbose=verbose)

    def resolve(self, key: Key, *, fetch: bool = True) -> Path:
        """ This is not safe, but it's fast. """
        return self.read(lambda path: path, key, fetch=fetch)


def normalize_levels(levels, cls):
    for entry in levels:
        if isinstance(entry, (str, Path)):
            entry = cls(entry)
        if isinstance(entry, cls):
            entry = entry,
        if not isinstance(entry, StorageLevel):
            entry = StorageLevel(entry, True, True)

        yield entry
