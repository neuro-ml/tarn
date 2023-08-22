import os

import numpy as np
import pytest
from pickler_test_helpers import functions

from tarn.cache import CacheIndex, CacheStorage, JsonSerializer, NumpySerializer
from tarn.config import StorageConfig, ToolConfig, init_storage
from tarn.functional import smart_cache
from tarn.pickler.compat import make_stable, make_unstable
from tarn.serializers import PickleSerializer


def some_func(x):
    return x ** 2


def test_read_write(storage_factory, temp_dir):
    with storage_factory() as storage:
        root = temp_dir / 'cache'
        init_storage(StorageConfig(hash='blake2b', levels=[1, 63], usage=ToolConfig(name='StatUsage')), root)
        cache = CacheStorage(CacheIndex(root, storage, JsonSerializer()))

        x = 10
        key = (some_func, x)
        value = some_func(x)
        default = set(root.glob('*/*'))

        _, success = cache.read(key, error=False)
        assert not success
        assert cache.write(key, value) is not None

        assert cache.read(key) == value
        # trigger consistency check
        assert cache.write(key, value) is not None

        # corrupt the index
        h, = set(root.glob('*/*')) - default
        os.chmod(h, 0o777)
        open(h, 'w').close()
        # make sure it was cleaned
        _, success = cache.read(key, error=False)
        assert not success


def test_corrupted_numpy(storage_factory, temp_dir):
    root = temp_dir / 'cache'
    storage_root = temp_dir / 'storage'
    with storage_factory(root=storage_root) as storage:
        init_storage(StorageConfig(hash='blake2b', levels=[1, 63], usage=ToolConfig(name='StatUsage')), root)
        cache = CacheStorage(CacheIndex(root, storage, NumpySerializer()))

        key = 10
        value = np.array([1, 2, 3])
        default = set(storage_root.glob('*/*/*'))

        cache.write(key, value)

        # trigger consistency check
        cache.write(key, value)

        # corrupt the index
        files = set(storage_root.glob('*/*/*')) - default
        assert len(files) == 1, files
        h, = files
        os.chmod(h, 0o777)
        open(h, 'w').close()
        # make sure it was cleaned
        value, success = cache.read(key, error=False)
        assert not success, (value, success)


def test_smart_cache(storage_factory, disk_dict_factory):
    with storage_factory() as storage:
        with disk_dict_factory() as index_disk_dict:
            cached_f = smart_cache(
                index_disk_dict,
                storage,
                PickleSerializer(),
            )(functions.calls_counter)
            cached_f(1)
            assert functions.calls_counter.counter == 1
            cached_f(1)
            assert functions.calls_counter.counter == 2

            make_stable(functions.calls_counter)
            cached_f = smart_cache(
                index_disk_dict,
                storage,
                PickleSerializer(),
            )(functions.calls_counter)
            cached_f(1)
            assert functions.calls_counter.counter == 3
            cached_f(1)
            assert functions.calls_counter.counter == 3

            cached_f = smart_cache(
                index_disk_dict,
                storage,
                PickleSerializer(),
                unstable_objects={functions.calls_counter}
            )(functions.calls_counter)
            cached_f(1)
            assert functions.calls_counter.counter == 4
            cached_f(1)
            assert functions.calls_counter.counter == 5

            make_unstable(functions.calls_counter)
            cached_f = smart_cache(
                index_disk_dict,
                storage,
                PickleSerializer(),
                stable_objects={functions.calls_counter}
            )(functions.calls_counter)
            cached_f(1)
            assert functions.calls_counter.counter == 5
            cached_f(1)
            assert functions.calls_counter.counter == 5
            cached_f(2)
            assert functions.calls_counter.counter == 6
            cached_f(1)
            assert functions.calls_counter.counter == 6
