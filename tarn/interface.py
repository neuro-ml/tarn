import logging
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional, Sequence, Iterable, BinaryIO, Union
from typing import Tuple, Callable, Any

Key = bytes
Keys = Sequence[Key]
PathOrStr = Union[Path, str, os.PathLike]
Value = Union[BinaryIO, PathOrStr]
MaybeValue = Optional[Value]

logger = logging.getLogger(__name__)


class LocalStorage(ABC):
    """
    Storage that has a well-defined location on the filesystem
    """

    def __init__(self, hash, levels: Sequence[int]):
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
    levels: Sequence[int]

    def fetch(self, keys: Sequence[Key], store: Callable[[Key, Path], Any],
              config) -> Iterable[Tuple[Any, bool]]:
        """
        Fetches the values for `keys` from a remote location.
        """
        raise NotImplementedError('Fetch is not supported by this backend')

    def push(self, keys: Sequence[Key], resolve: Callable[[Key], Path], config) -> Iterable[bool]:
        """
        Pushes the values for `keys` to a remote location.
        """
        raise NotImplementedError('Push is not supported by this backend')
