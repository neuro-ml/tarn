import os

from tarn.cache import CacheStorage, CacheIndex, JsonSerializer
from tarn.config import init_storage, StorageConfig, ToolConfig


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

        _, success = cache.read(key, error=False)
        assert not success
        cache.write(key, value)

        assert cache.read(key) == value
        # trigger consistency check
        cache.write(key, value)

        # corrupt the index
        h, = root.glob('**/hash.bin')
        os.chmod(h, 0o777)
        os.remove(h)
        # make sure it was cleaned
        _, success = cache.read(key, error=False)
        assert not success
