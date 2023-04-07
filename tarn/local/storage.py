import logging
import warnings
from pathlib import Path
from typing import Sequence, Iterable, Union, Optional

from .disk import Disk
from ..location import Level, Levels, Location, Fanout
from ..interface import Key
from ..pool import HashKeyStorage
from ..pool.hash_key import resolve_location
from ..utils import PathLike

logger = logging.getLogger(__name__)


class Storage(HashKeyStorage):
    def __init__(self, *local: Union[Level, Sequence[Disk], Disk, Sequence[PathLike], PathLike],
                 remote: Sequence[Location] = ()):
        warnings.warn('This interface is deprecated. Use `HashKeyStorage` instead', UserWarning)
        warnings.warn('This interface is deprecated. Use `HashKeyStorage` instead', DeprecationWarning)
        super().__init__(Levels(*map(_resolve, local)), remote)

    @property
    def levels(self):
        return self._local._levels

    def write(self, value, error: bool = True) -> Optional[str]:
        result = super().write(value, error)
        if result is not None:
            return result.hex()

    def fetch(self, keys: Sequence[Key], *, verbose: bool, legacy: bool = True) -> Iterable[Key]:
        """ Fetch the `keys` from remote. Yields the keys that were successfully fetched """
        for key, success in super().fetch(keys):
            if (success and not legacy) or (not success and legacy):
                yield key

    def resolve(self, key: Key, *, fetch: bool = True) -> Path:
        """ This is not safe, but it's fast. """
        return self.read(lambda path: path, key, fetch=fetch)


def _resolve(x):
    if isinstance(x, Level):
        return x
    return resolve_location(x)


def normalize_levels(levels, cls):
    for entry in levels:
        if isinstance(entry, (str, Path)):
            entry = cls(entry)
        if isinstance(entry, cls):
            entry = entry,
        if not isinstance(entry, Level):
            entry = Level(Fanout(*entry), True, True)

        yield entry
