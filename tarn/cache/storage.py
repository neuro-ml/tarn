import logging

from . import CacheIndex
from .pickler import dumps
from .. import Level, Levels, Fanout
from ..local.storage import normalize_levels, Storage
from ..pool.hash_key import resolve_location, HashKeyStorage
from ..pool.pickle_key import PickleKeyStorage

logger = logging.getLogger(__name__)


class CacheStorage(PickleKeyStorage):
    def __init__(self, *local, remote=()):
        levels = list(normalize_levels(local, CacheIndex))
        cache_index = levels[0].location._locations[0]
        if remote:
            levels.append(Fanout(*remote))
        storage = cache_index.storage
        if isinstance(storage, Storage):
            storage = HashKeyStorage(storage._local, storage._remote, storage._fetch, storage._error, storage.algorithm)
        super().__init__(Levels(*levels), storage=storage, serializer=cache_index.serializer)

    def read(self, key, *, error: bool = True, fetch=None):
        return super(CacheStorage, self).read(key, error=error)


def key_to_digest(algorithm, key, version=None):
    pickled = dumps(key, version=version)
    digest = algorithm(pickled).digest()
    return pickled, digest


def _resolve(x):
    if not isinstance(x, Level):
        return Level(x, write=True)
    return resolve_location(x)
