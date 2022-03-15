import logging
from abc import ABC, abstractmethod
from pathlib import Path

from redis import Redis

__all__ = 'SizeTracker', 'DummySize', 'RedisSize'

logger = logging.getLogger(__name__)


class SizeTracker(ABC):
    """ Used to store information about the volume occupied by the storage """

    @abstractmethod
    def get(self, root: Path):
        pass

    @abstractmethod
    def set(self, size: int, root: Path):
        pass

    @abstractmethod
    def inc(self, size: int, root: Path):
        pass

    @abstractmethod
    def dec(self, size: int, root: Path):
        pass


class DummySize(SizeTracker):
    def get(self, root: Path):
        return 0

    def set(self, size: int, root: Path):
        pass

    def inc(self, size: int, root: Path):
        pass

    def dec(self, size: int, root: Path):
        pass


class RedisSize(SizeTracker):
    def __init__(self, *args, prefix: str):
        if len(args) == 1 and isinstance(args[0], Redis):
            redis, = args
        else:
            redis = Redis(*args)

        self._redis = redis
        self._volume_key = f'{prefix}.S'

    def get(self, root: Path):
        return int(self._redis.get(self._volume_key) or 0)

    def set(self, size: int, root: Path):
        self._redis.set(self._volume_key, size)

    def inc(self, size: int, root: Path):
        self._redis.incrby(self._volume_key, size)

    def dec(self, size: int, root: Path):
        self._redis.decrby(self._volume_key, size)

    @classmethod
    def from_url(cls, url: str, prefix: str):
        return cls(Redis.from_url(url), prefix=prefix)
