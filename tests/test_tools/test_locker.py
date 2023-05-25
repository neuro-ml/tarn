import time
from multiprocessing.pool import ThreadPool
from threading import Thread
from multiprocessing.context import Process

import cloudpickle
import pytest

from tarn.tools import RedisLocker


@pytest.mark.redis
def test_redis_expire(redis_hostname):
    locker = RedisLocker(redis_hostname, prefix='', expire=1)
    with pytest.raises(RuntimeError, match=r'The locker is in a wrong state \(None\). Did it expire?'):
        with locker.read(b'\x00'):
            time.sleep(2)

    with pytest.raises(RuntimeError, match=r'The locker is in a wrong state \(None\). Did it expire?'):
        with locker.write(b'\x00'):
            time.sleep(2)


@pytest.mark.redis
def test_redis_pickle(redis_hostname):
    locker = RedisLocker(redis_hostname, prefix='', expire=1)
    x = cloudpickle.loads(cloudpickle.dumps(locker))
    xx = cloudpickle.loads(cloudpickle.dumps(x))

    assert x._redis.get_connection_kwargs() == xx._redis.get_connection_kwargs()
    assert x._prefix == xx._prefix == b':'
    assert x._expire == xx._expire == locker._expire == 1


@pytest.mark.redis
def test_parallel_read_threads(storage_factory, subtests, redis_hostname):
    def job():
        storage.read(lambda x: time.sleep(sleep_time), key)

    sleep_time = 1
    lockers = [
        {'name': 'GlobalThreadLocker'},
        {'name': 'RedisLocker', 'args': [redis_hostname], 'kwargs': {'prefix': 'tarn.tests', 'expire': 10}},
    ]

    for locker in lockers:
        with subtests.test(locker['name']), storage_factory(locker) as storage:
            key = storage.write(__file__)
            # single thread
            th = Thread(target=job)
            th.start()
            job()
            th.join()

            # thread pool
            pool = ThreadPool(10, job)
            pool.close()
            pool.join()


@pytest.mark.redis
def test_parallel_read_processes(storage_factory, redis_hostname):
    def job():
        storage.read(lambda x: time.sleep(1), key)

    with storage_factory({'name': 'RedisLocker', 'args': [redis_hostname],
                          'kwargs': {'prefix': 'tarn.tests', 'expire': 10}}) as storage:
        key = storage.write(__file__)

        start = time.time()
        th = Process(target=job)
        th.start()
        job()
        th.join()
        stop = time.time()

        assert stop - start < 1.5
