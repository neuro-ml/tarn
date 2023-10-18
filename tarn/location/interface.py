from abc import ABC, abstractmethod
from typing import ContextManager, Iterable, Sequence, Tuple, Union

from ..compat import Self
from ..interface import Key, Keys, MaybeLabels, MaybeValue, Meta, Value


class Location(ABC):
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
        for key in keys:
            with self.read(key, True) as value:
                yield key, value

    @abstractmethod
    def contents(self) -> Iterable[Tuple[Key, Self, Meta]]:
        pass

    @abstractmethod
    def write(self, key: Key, value: Value, labels: MaybeLabels) -> ContextManager[MaybeValue]:
        pass

    @abstractmethod
    def delete(self, key: Key) -> bool:
        pass

    @abstractmethod
    def touch(self, key: Key) -> bool:
        """
        Update usage date for a given `key`
        """
        pass


class ReadOnly(Location):
    def write(self, key: Key, value: Value, labels: MaybeLabels) -> ContextManager[MaybeValue]:
        yield None

    def delete(self, key: Key) -> bool:
        return False

    def touch(self, key: Key) -> bool:
        return False


Locations = Sequence[Location]
