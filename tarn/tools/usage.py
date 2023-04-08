from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Optional

from ..compat import get_path_group, remove_file, set_path_attrs
from ..digest import key_to_relative
from ..interface import Key

__all__ = 'UsageTracker', 'DummyUsage', 'StatUsage'


class UsageTracker(ABC):
    def __init__(self, root: Path):
        self.root = root

    @abstractmethod
    def update(self, key: Key, path: Path):
        """ Updates the usage time for a given `key` """

    @abstractmethod
    def get(self, key: Key, path: Path) -> Optional[datetime]:
        """ Deletes the usage time for a given `key` """

    @abstractmethod
    def delete(self, key: Key):
        """ Deletes the usage time for a given `key` """


class DummyUsage(UsageTracker):
    def update(self, key: Key, path: Path):
        pass

    def get(self, key: Key, path: Path) -> Optional[datetime]:
        return None

    def delete(self, key: Key):
        pass


class StatUsage(UsageTracker):
    def update(self, key: Key, path: Path):
        mark = self._mark(key)
        missing = not mark.exists()
        mark.touch(exist_ok=True)
        if missing:
            set_path_attrs(mark, 0o777, get_path_group(path))

    def delete(self, key: Key):
        mark = self._mark(key)
        if mark.exists():
            remove_file(mark)

    def get(self, key: Key, path: Path) -> Optional[datetime]:
        mark = self._mark(key)
        if mark.exists():
            return datetime.fromtimestamp(mark.stat().st_mtime)

    def _mark(self, key):
        mark = self.root / key_to_relative(key, (1, len(key) - 1))
        mark.parent.mkdir(parents=True, exist_ok=True)
        return mark
