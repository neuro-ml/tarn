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
    def last_used(self, key: Key, base: Path) -> Union[datetime, None]:
        """ Deletes the usage time for a given ``key`` """


class DummyUsage(UsageTracker):
    def update(self, key: Key, base: Path):
        pass

    def delete(self, key: Key, base: Path):
        pass

    def last_used(self, key: Key, base: Path) -> Union[datetime, None]:
        return None


class StatUsage(UsageTracker):
    def update(self, key: Key, base: Path):
        path = base / '.time'
        missing = not path.exists()
        path.touch(exist_ok=True)
        if missing:
            os.chmod(path, 0o777)
            shutil.chown(path, group=base.group())

    def delete(self, key: Key, base: Path):
        os.remove(base / '.time')

    def last_used(self, key: Key, base: Path) -> Union[datetime, None]:
        return datetime.fromtimestamp((base / '.time').stat().st_mtime)
