import logging
import time
from abc import ABC, abstractmethod
from contextlib import contextmanager
from threading import Lock
from typing import AnyStr, ContextManager, Optional

from redis import Redis
from redis.exceptions import NoScriptError

from ..interface import Key

__all__ = 'Locker', 'DummyLocker', 'RedisLocker', 'GlobalThreadLocker'

logger = logging.getLogger(__name__)


class Locker(ABC):
    @abstractmethod
    def read(self, key: Key) -> ContextManager[None]:
        pass

    @abstractmethod
    def write(self, key: Key) -> ContextManager[None]:
        pass


class PotentialDeadLock(RuntimeError):
    pass


class DummyLocker(Locker):
    @contextmanager
    def read(self, key: Key) -> ContextManager[None]:
        yield

    write = read


class GlobalThreadLocker(Locker):
    def __init__(self, timeout: Optional[int] = None):
        self.timeout = timeout
        self._lock = Lock()

    @contextmanager
    def read(self, key: Key) -> ContextManager[None]:
        success = self._lock.acquire(timeout=-1 if self.timeout is None else self.timeout)
        if not success:
            raise PotentialDeadLock(f"It seems like you've hit a deadlock for key {key}.")

        try:
            yield
        finally:
            self._lock.release()

    write = read


class RedisLocker(Locker):
    def __init__(self, *args, prefix: AnyStr, expire: int, **kwargs):
        if len(args) == 1 and isinstance(args[0], Redis):
            assert not kwargs, kwargs
            redis, = args
        else:
            redis = Redis(*args, **kwargs)
        if isinstance(prefix, str):
            prefix = prefix.encode()

        self._redis = redis
        self._prefix = prefix + b':'
        self._expire = expire
        self._volume_key = prefix + b'.V'
        self._update_scripts()

    @classmethod
    def from_url(cls, url: str, prefix: AnyStr, expire: int):
        return cls(Redis.from_url(url), prefix=prefix, expire=expire)

    @contextmanager
    def read(self, key: Key) -> ContextManager[None]:
        sleep_time = 0.01
        sleep_iters = int(self._expire / sleep_time) or 1
        wait_for_true(self._start_reading, key, sleep_time, sleep_iters)

        try:
            yield
        finally:
            lock = self._safe_eval(self._stop_reading_script, 1, self._prefix + key)
        if lock != b'1':
            raise RuntimeError(f'The locker is in a wrong state ({lock}). Did it expire?')

    @contextmanager
    def write(self, key: Key) -> ContextManager[None]:
        sleep_time = 0.01
        sleep_iters = int(self._expire / sleep_time) or 1
        wait_for_true(self._start_writing, key, sleep_time, sleep_iters)

        try:
            yield
        finally:
            lock = self._safe_eval(self._stop_writing_script, 1, self._prefix + key)
        if lock != b'-1':
            raise RuntimeError(f'The locker is in a wrong state ({lock}). Did it expire?')

    def _update_scripts(self):
        expire = self._expire
        # TODO: how slow are these checks?
        # language=Lua
        self._stop_writing_script = self._redis.script_load('''
        local lock = redis.call('get', KEYS[1])
        if lock == '-1' then
            redis.call('del', KEYS[1])
        end
        return lock''')
        # language=Lua
        self._start_reading_script = self._redis.script_load(f'''
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
        self._stop_reading_script = self._redis.script_load(f'''
        local lock = redis.call('get', KEYS[1])
        if lock == false then 
            return lock
        elseif lock == '1' then
            redis.call('del', KEYS[1])
            return lock
        elseif tonumber(lock) < 1 then
            return lock
        else
            redis.call('set', KEYS[1], lock - 1, 'EX', {expire})
            return '1'
        end''')

    def _safe_eval(self, *args):
        try:
            return self._redis.evalsha(*args)
        except NoScriptError:
            pass

        self._update_scripts()
        return self._redis.evalsha(*args)

    def _start_writing(self, key: Key) -> bool:
        return bool(self._redis.set(self._prefix + key, -1, nx=True, ex=self._expire))

    def _start_reading(self, key: Key) -> bool:
        return bool(self._safe_eval(self._start_reading_script, 1, self._prefix + key))

    @classmethod
    def _from_args(cls, prefix, expire, kwargs):
        return cls(prefix=prefix, expire=expire, **kwargs)

    def __reduce__(self):
        return self._from_args, (self._prefix[:-1], self._expire, self._redis.get_connection_kwargs())


def wait_for_true(func, key, sleep_time, max_iterations):
    i = 0
    while not func(key):
        if i >= max_iterations:
            logger.error('Potential deadlock detected for %s', key)
            raise PotentialDeadLock(f"It seems like you've hit a deadlock for key {key}.")

        time.sleep(sleep_time)
        i += 1

    logger.debug('Waited for %d iterations for %s', i, key)
