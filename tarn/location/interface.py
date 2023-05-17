from abc import ABC, abstractmethod
from typing import ContextManager, Iterable, Optional, Sequence, Tuple, Type

from ..compat import HashAlgorithm
from ..interface import Key, Keys, MaybeValue, Value, MaybeLabels


class Location(ABC):
    key_size: Optional[int]
    hash: Optional[Type[HashAlgorithm]]

    @abstractmethod
    def read(self, key: Key) -> ContextManager[MaybeValue]:
        pass

    @abstractmethod
    def read_batch(self, keys: Keys) -> Iterable[Tuple[Key, MaybeValue]]:
        pass


class Writable(Location, ABC):
    @abstractmethod
    def write(self, key: Key, value: Value, labels: MaybeLabels) -> ContextManager[MaybeValue]:
        pass

    @abstractmethod
    def delete(self, key: Key) -> bool:
        pass


Locations = Sequence[Location]
