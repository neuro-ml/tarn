import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import pytest

from tarn import Storage, DiskDict, PickleKeyStorage
from tarn.config import init_storage, StorageConfig
from tarn.pool import HashKeyStorage

pytest_plugins = 'cache_fixtures',


@pytest.fixture
def inside_ci():
    return 'CI' in os.environ


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
            for name in names or [Path()]:
                local = root / name
                roots.append(local)
                init_storage(StorageConfig(
                    hash='blake2b', levels=[1, 63], locker=locker
                ), local, exist_ok=exist_ok, group=group)

            yield HashKeyStorage(list(map(DiskDict, roots)))

    return factory


@pytest.fixture
def disk_dict_factory():
    @contextmanager
    def factory(locker=None, group=None, root=None, exist_ok=False) -> Iterator[Storage]:
        with _safe_tmpdir() as _root:
            if root is None:
                root = _root
                exist_ok = True
            root = Path(root)

            init_storage(StorageConfig(
                hash='blake2b', levels=[1, 63], locker=locker
            ), root, exist_ok=exist_ok, group=group)

            yield DiskDict(root)

    return factory


@pytest.fixture
def random_disk_dict(disk_dict_factory) -> DiskDict:
    with disk_dict_factory() as disk:
        yield disk


@pytest.fixture
def disk_cache_factory(storage_factory):
    @contextmanager
    def factory(serializer) -> Iterator[PickleKeyStorage]:
        with _safe_tmpdir() as root, storage_factory() as storage:
            local = root / 'cache'
            init_storage(StorageConfig(hash='blake2b', levels=[1, 63]), local)
            yield PickleKeyStorage(local, storage, serializer)

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
