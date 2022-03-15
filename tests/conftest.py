import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import pytest

from pond import Storage, Disk
from pond.cache import CacheStorage, CacheIndex
from pond.config import init_storage, StorageConfig

pytest_plugins = 'cache_fixtures',


@pytest.fixture
def redis_hostname():
    return 'localhost'


@pytest.fixture
def tests_root():
    return Path(__file__).resolve().parent


@pytest.fixture
def temp_dir(tmpdir):
    return Path(tmpdir)


@pytest.fixture
def chdir():
    @contextmanager
    def internal(folder):
        current = os.getcwd()
        try:
            os.chdir(folder)
            yield
        finally:
            os.chdir(current)

    return internal


@pytest.fixture
def storage_factory():
    @contextmanager
    def factory(locker=None, group=None, names=('storage',)) -> Iterator[Storage]:
        with tempfile.TemporaryDirectory() as root:
            roots = []
            for name in names:
                local = Path(root) / name
                roots.append(local)
                init_storage(StorageConfig(hash='blake2b', levels=[1, 63], locker=locker), local, group=group)

            yield Storage(*map(Disk, roots))

    return factory


@pytest.fixture
def disk_cache_factory(storage_factory):
    @contextmanager
    def factory(serializer) -> Iterator[CacheStorage]:
        with tempfile.TemporaryDirectory() as root, storage_factory() as storage:
            roots = []
            local = Path(root) / 'cache'
            roots.append(local)
            init_storage(StorageConfig(hash='blake2b', levels=[1, 63]), local)

            yield CacheStorage(*(CacheIndex(x, storage, serializer) for x in roots))

    return factory
