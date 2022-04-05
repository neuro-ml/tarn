import logging
from typing import Sequence, Union, Any, NamedTuple

from ..exceptions import ReadError
from ..interface import StorageBase, RemoteStorage, WriteError, StorageLevel
from ..local.storage import normalize_levels
from ..utils import PathLike, Key
from .pickler import PREVIOUS_VERSIONS, dumps
from .index import CacheIndex

logger = logging.getLogger(__name__)

LocalType = Union[StorageLevel, Sequence[CacheIndex], CacheIndex, Sequence[PathLike], PathLike]
ProxyKey = Any


class _PreparedKey(NamedTuple):
    digest: str
    raw: Any
    pickled: bytes


class CacheStorage:
    def __init__(self, *local: LocalType, remote: Sequence[RemoteStorage] = ()):
        normalized = tuple(normalize_levels(local, CacheIndex))

        self.storage = StorageBase(*normalized, remote=remote)
        self.algorithm = self.storage.hash.build()
        self.digest_size = sum(normalized[0].locations[0].levels)

    @property
    def levels(self) -> Sequence[StorageLevel]:
        return self.storage.levels

    def prepare(self, key: ProxyKey) -> _PreparedKey:
        pickled, digest = key_to_digest(self.algorithm, key)
        return _PreparedKey(digest, key, pickled)

    def read(self, key: ProxyKey, *, fetch: bool = True, error: bool = True):
        if not isinstance(key, _PreparedKey):
            key = self.prepare(key)

        digest = key.digest
        value, exists = self._read(digest, key.pickled, key.raw, fetch)
        if not error:
            return value, bool(exists)

        if exists:
            return value
        if exists is None:
            raise WriteError(f"The key {digest} couldn't be written to any storage")
        if fetch:
            message = f'Key {digest} is not present neither locally nor among your {len(self.storage.remote)} remotes'
        else:
            message = f'Key {digest} is not present locally'
        raise ReadError(message)

    def write(self, key: ProxyKey, value: Any, *, error: bool = True) -> Union[Key, None]:
        if not isinstance(key, _PreparedKey):
            key = self.prepare(key)

        digest = key.digest
        logger.info('Saving key %s', digest)
        if not self.storage.write(digest, value, key.pickled):
            if error:
                raise WriteError('The file could not be written to any storage')
            return None

        return digest

    def _read(self, digest, pickled, raw, fetch):
        value, exists = self.storage.read(digest, pickled, fetch=fetch)
        if exists:
            logger.info('Key %s found', digest)
            return value, True

        # the cache is empty, but we can try and restore it from an older version
        for version in reversed(PREVIOUS_VERSIONS):
            local_pickled, local_digest = key_to_digest(self.algorithm, raw, version)

            # we can simply load the previous version, because nothing really changed
            value, exists = self.storage.read(local_digest, local_pickled, fetch=fetch)
            if exists:
                logger.info('Key %s found in previous version (%d). Updating', digest, version)
                # and store it for faster access next time
                self.storage.write(digest, value, pickled)
                return value, True

        logger.info('Key %s not found', digest)
        return None, False


def key_to_digest(algorithm, key, version=None):
    pickled = dumps(key, version=version)
    digest = algorithm(pickled).hexdigest()
    return pickled, digest
