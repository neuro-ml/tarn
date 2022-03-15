import logging
import time
from abc import ABC, abstractmethod
from contextlib import contextmanager
from pathlib import Path
from threading import Lock

from redis import Redis

from ..utils import Key

__all__ = 'Locker', 'DummyLocker', 'RedisLocker', 'GlobalThreadLocker'

logger = logging.getLogger(__name__)


class Locker(ABC):
    @contextmanager
    def read(self, key: Key, base: Path):
        self.reserve_read(key, base)
        try:
            yield
        finally:
            self.stop_reading(key, base)

    @contextmanager
    def write(self, key: Key, base: Path):
        self.reserve_write(key, base)
        try:
            yield
        finally:
            self.stop_writing(key, base)

    def reserve_read(self, key: Key, base: Path):
        sleep_time = 0.1
        sleep_iters = int(600 / sleep_time) or 1  # 10 minutes
        wait_for_true(self.start_reading, key, base, sleep_time, sleep_iters)

    def reserve_write(self, key: Key, base: Path):
        sleep_time = 0.1
        sleep_iters = int(600 / sleep_time) or 1  # 10 minutes
        wait_for_true(self.start_writing, key, base, sleep_time, sleep_iters)

    @abstractmethod
    def start_reading(self, key: Key, base: Path) -> bool:
        """ Try to reserve a read operation. Return True if it was successful. """

    @abstractmethod
    def stop_reading(self, key: Key, base: Path):
        """ Release a read operation. """

    @abstractmethod
    def start_writing(self, key: Key, base: Path) -> bool:
        """ Try to reserve a write operation. Return True if it was successful. """

    @abstractmethod
    def stop_writing(self, key: Key, base: Path):
        """ Release a write operation. """


class DummyLocker(Locker):
    def start_reading(self, key: Key, base: Path) -> bool:
        return True

    def stop_reading(self, key: Key, base: Path):
        pass

    def start_writing(self, key: Key, base: Path) -> bool:
        return True

    def stop_writing(self, key: Key, base: Path):
        pass


class GlobalThreadLocker(Locker):
    def __init__(self):
        self._lock = Lock()

    def _acquire(self):
        if self._lock.locked():
            return False
        self._lock.acquire()
        return True

    def _release(self):
        assert self._lock.locked()
        self._lock.release()

    def start_reading(self, key: Key, base: Path) -> bool:
        return self._acquire()

    def stop_reading(self, key: Key, base: Path):
        self._release()

    def start_writing(self, key: Key, base: Path) -> bool:
        return self._acquire()

    def stop_writing(self, key: Key, base: Path):
        self._release()


class RedisLocker(Locker):
    def __init__(self, *args, prefix: str, expire: int):
        if len(args) == 1 and isinstance(args[0], Redis):
            redis, = args
        else:
            redis = Redis(*args)

        self._redis = redis
        self._prefix = prefix + ':'
        self._expire = expire
        self._volume_key = f'{prefix}.V'
        # TODO: how slow are these checks?
        # language=Lua
        self._stop_writing = self._redis.script_load('''
        if redis.call('get', KEYS[1]) == '-1' then
            redis.call('del', KEYS[1])
        else
            error('')
        end''')
        # language=Lua
        self._start_reading = self._redis.script_load(f'''
        local lock = redis.call('get', KEYS[1])
        if lock == '-1' then 
            return 0
        elseif lock == false then
            redis.call('set', KEYS[1], 1, 'EX', {expire})
            return 1
        else
            redis.call('set', KEYS[1], lock + 1, 'EX', {expire})
            return 1
        end''')
        # language=Lua
        self._stop_reading = self._redis.script_load(f'''
        local lock = redis.call('get', KEYS[1])
        if lock == '1' then
            redis.call('del', KEYS[1])
        elseif tonumber(lock) < 1 then
            error('')
        else
            redis.call('set', KEYS[1], lock - 1, 'EX', {expire})
        end''')

    def start_writing(self, key: Key, base: Path) -> bool:
        return bool(self._redis.set(self._prefix + key, -1, nx=True, ex=self._expire))

    def stop_writing(self, key: Key, base: Path):
        self._redis.evalsha(self._stop_writing, 1, self._prefix + key)

    def start_reading(self, key: Key, base: Path) -> bool:
        return bool(self._redis.evalsha(self._start_reading, 1, self._prefix + key))

    def stop_reading(self, key: Key, base: Path):
        self._redis.evalsha(self._stop_reading, 1, self._prefix + key)

    @classmethod
    def from_url(cls, url: str, prefix: str, expire: int):
        return cls(Redis.from_url(url), prefix=prefix, expire=expire)


class PotentialDeadLock(RuntimeError):
    pass


def wait_for_true(func, key, base, sleep_time, max_iterations):
    i = 0
    while not func(key, base):
        if i >= max_iterations:
            logger.error('Potential deadlock detected for %s', key)
            raise PotentialDeadLock(f"It seems like you've hit a deadlock for key {key}.")

        time.sleep(sleep_time)
        i += 1

    logger.debug('Waited for %d iterations for %s', i, key)
