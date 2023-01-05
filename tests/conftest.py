import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import pytest

from tarn import Storage, Disk
from tarn.cache import CacheStorage, CacheIndex
from tarn.config import init_storage, StorageConfig

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
    def factory(locker=None, group=None, names=('storage',), root=None, exist_ok=False) -> Iterator[Storage]:
        with _safe_tmpdir() as _root:
            if root is None:
                root = _root
            root = Path(root)

            roots = []
            for name in names:
                local = root / name
                roots.append(local)
                init_storage(StorageConfig(
                    hash='blake2b', levels=[1, 63], locker=locker
                ), local, exist_ok=exist_ok, group=group)

            yield Storage(*map(Disk, roots))

    return factory


@pytest.fixture
def disk_cache_factory(storage_factory):
    @contextmanager
    def factory(serializer) -> Iterator[CacheStorage]:
        with _safe_tmpdir() as root, storage_factory() as storage:
            roots = []
            local = root / 'cache'
            roots.append(local)
            init_storage(StorageConfig(hash='blake2b', levels=[1, 63]), local)

            yield CacheStorage(*(CacheIndex(x, storage, serializer) for x in roots))

    return factory


# py<3.8 has a bug in rmtree for windows
@contextmanager
def _safe_tmpdir():
    tmp = tempfile.TemporaryDirectory()
    yield Path(tmp.name)
    try:
        tmp.cleanup()
    except PermissionError:
        pass
