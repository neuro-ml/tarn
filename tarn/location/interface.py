from abc import ABC, abstractmethod
from typing import ContextManager, Iterable, Optional, Sequence, Tuple, Type, Union

from ..compat import HashAlgorithm, Self
from ..interface import Key, Keys, MaybeLabels, MaybeValue, Meta, Value


class Location(ABC):
    key_size: Optional[int]
    hash: Optional[Type[HashAlgorithm]]

    @abstractmethod
    def read(self, key: Key, return_labels: bool) -> ContextManager[Union[None, Value, Tuple[Value, MaybeLabels]]]:
        """
        Read a value for a given `key`. If the result is None - the key was not found.

        Examples
        --------
        >>> with location.read(key) as result:
        ...     if result is None:
        ...        print('not found')
        ...     else:
        ...         with open(result) as file:
        ...             print('found', file.read())
        """

    @abstractmethod
    def read_batch(self, keys: Keys) -> Iterable[Tuple[Key, Union[None, Tuple[Value, MaybeLabels]]]]:
        """
        Reads multiple values given a collection of `keys`.

        Examples
        --------
        >>> for key, result in location.read_batch(keys):
        ...     if result is None:
        ...        print(key, 'not found')
        ...     else:
        ...        print(key, 'found')
        """

    @abstractmethod
    def contents(self) -> Iterable[Tuple[Key, Self, Meta]]:
        pass


class Writable(Location, ABC):
    @abstractmethod
    def write(self, key: Key, value: Value, labels: MaybeLabels) -> ContextManager[MaybeValue]:
        pass

    @abstractmethod
    def delete(self, key: Key) -> bool:
        pass


Locations = Sequence[Location]
