import os
import shutil
from datetime import datetime
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Union

from ..utils import Key

__all__ = 'UsageTracker', 'DummyUsage'


class UsageTracker(ABC):
    @abstractmethod
    def update(self, key: Key, base: Path):
        """ Updates the usage time for a given ``key`` """

    @abstractmethod
    def delete(self, key: Key, base: Path):
        """ Deletes the usage time for a given ``key`` """

    @abstractmethod
    def get(self, key: Key, base: Path) -> Union[datetime, None]:
        """ Deletes the usage time for a given ``key`` """


class DummyUsage(UsageTracker):
    def update(self, key: Key, base: Path):
        pass

    def delete(self, key: Key, base: Path):
        pass

    def get(self, key: Key, base: Path) -> Union[datetime, None]:
        return None


class StatUsage(UsageTracker):
    def update(self, key: Key, base: Path):
        mark = self._mark(base)
        missing = not mark.exists()
        mark.touch(exist_ok=True)
        if missing:
            os.chmod(mark, 0o777)
            shutil.chown(mark, group=base.group())

    def delete(self, key: Key, base: Path):
        mark = self._mark(base)
        if mark.exists():
            os.remove(mark)

    def get(self, key: Key, base: Path) -> Union[datetime, None]:
        mark = self._mark(base)
        if mark.exists():
            stamp = mark.stat().st_mtime
        else:
            stamp = base.stat().st_mtime
        return datetime.fromtimestamp(stamp)

    @staticmethod
    def _mark(base: Path):
        return base / '.time'
