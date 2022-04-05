import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Iterable, Tuple, Callable, Any, Sequence, Union, NamedTuple, Optional

from tqdm.auto import tqdm

from .config import HashConfig
from .exceptions import WriteError
from .utils import Key

logger = logging.getLogger(__name__)


class LocalStorage(ABC):
    """
    Storage that has a well-defined location on the filesystem
    """

    def __init__(self, hash: HashConfig, levels: Sequence[int]):
        self.hash = hash
        self.algorithm = self.hash.build()
        self.levels = levels

    @abstractmethod
    def write(self, key, value: Any, context: Any) -> bool:
        """
        Write a ``value`` to a given ``key``.
        Returns True if the value was written (or already present).
        """

    @abstractmethod
    def read(self, key, context: Any) -> Tuple[Any, bool]:
        """
        Read the value given the ``key``.
        Returns a pair (value, success).
        If success is False - the value could not be read.
        """

    @abstractmethod
    def delete(self, key, context: Any) -> bool:
        """
        Delete the value for the given ``key``.
        Returns True if the value was present and deleted.
        """

    @abstractmethod
    def contains(self, key, context: Any) -> bool:
        """
        Returns whether the ``key`` is present in the storage.
        """

    @abstractmethod
    def replicate_to(self, key, context: Any, store: Callable[[Key, Path], Any]):
        """
        Prepares a location for replication containing the given ``key``.
        """

    @abstractmethod
    def replicate_from(self, key, source: Path, context: Any) -> bool:
        """
        Populates the storage at ``key`` from a ``source`` path generated by `replicate_to`.
        """


class RemoteStorage:
    hash: HashConfig
    levels: Sequence[int]

    @abstractmethod
    def fetch(self, keys: Sequence[Key], store: Callable[[Key, Path], Any],
              config: HashConfig) -> Sequence[Tuple[Any, bool]]:
        """
        Fetches the value for ``key`` from a remote location.
        """


class StorageLevel(NamedTuple):
    locations: Sequence[LocalStorage]
    write: bool
    replicate: bool
    name: Optional[str] = None


class StorageBase:
    def __init__(self, *levels: StorageLevel, remote: Sequence[RemoteStorage] = ()):
        if not levels:
            raise ValueError('The storage must have at least 1 storage level')
        if not all(x.locations for x in levels):
            raise ValueError('Each level must have at least 1 location')

        reference = levels[0].locations[0].hash
        for layer in levels:
            for loc in layer.locations:
                if loc.hash != reference:
                    raise ValueError('Storage locations have inconsistent hash algorithms')

        self.hash = reference
        self.levels: Sequence[StorageLevel] = levels
        self.remote: Sequence[RemoteStorage] = tuple(remote)

    def write(self, key, value, context) -> bool:
        """
        Returns True if the ``value`` was written or already present.
        """
        for layer in self.levels:
            if layer.write:
                for location in layer.locations:
                    if location.write(key, value, context):
                        return True

        return False

    def read(self, key, context, *, fetch: bool) -> Tuple[Any, Union[None, bool]]:
        replicate = []
        # visit each level
        for layer in self.levels:
            for location in layer.locations:
                value, success = location.read(key, context)
                if success:
                    location.replicate_to(key, context, lambda k, base: self._replicate(k, base, context, replicate))
                    return value, True

            # nothing found in this level
            if layer.replicate:
                replicate.append(layer)

        # try to fetch from remote
        status = False
        if fetch:
            for remote in self.remote:
                (local, success), = remote.fetch(
                    [key], lambda k, base: self._replicate(k, base, context, replicate), self.hash
                )
                if success:
                    if local is WriteError:
                        status = None
                        continue

                    value, exists = local.read(key, context)
                    assert exists, exists
                    return value, True

        return None, status

    def fetch(self, keys: Iterable[Key], context, *, verbose: bool) -> Sequence[Key]:
        def store(k, base):
            status = self._replicate(k, base, context, self.levels)
            bar.update()
            return status if status is WriteError else k

        keys = set(keys)
        bar = tqdm(disable=not verbose, total=len(keys))
        present = set()
        for key in keys:
            if self._contains(key, context):
                present.add(key)
                bar.update()

        keys -= present
        logger.info(f'Fetch: {len(present)} keys already present, fetching {len(keys)}')

        for remote in self.remote:
            if not keys:
                break

            logger.info(f'Trying remote {remote}')
            keys -= {
                k for k, success in remote.fetch(list(keys), store, self.hash)
                if success and k is not WriteError
            }

        return list(keys)

    def _contains(self, key, context):
        for layer in self.levels:
            for location in layer.locations:
                if location.contains(key, context):
                    return True

        return False

    @staticmethod
    def _replicate(key, base, context, to_replicate):
        replicated = []
        for layer in to_replicate:
            if layer.replicate:
                for location in layer.locations:
                    if location.replicate_from(key, base, context):
                        replicated.append(location)
                        break

        if replicated:
            return replicated[0]

        return WriteError
