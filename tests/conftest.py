import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import pytest

from stash import Storage, Disk
from stash.config import init_storage, StorageConfig

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
                root = Path(root) / name
                roots.append(root)
                init_storage(
                    StorageConfig(
                        hash='blake2b', levels=[1, 63], locker=locker,
                    ), root, group=group,
                )

            yield Storage(list(map(Disk, roots)))

    return factory
