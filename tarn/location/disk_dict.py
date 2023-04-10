import logging
import os
import shutil
from contextlib import contextmanager
from pathlib import Path
from typing import ContextManager, Iterable, Optional, Tuple

from ..compat import copy_file, remove_file, rmtree
from ..config import load_config, root_params
from ..digest import key_to_relative
from ..exceptions import StorageCorruption
from ..interface import Key, Keys, MaybeValue, PathOrStr, Value
from ..tools import Locker, SizeTracker, UsageTracker
from ..utils import adjust_permissions, create_folders, get_size, match_buffers, match_files
from .interface import Writable

logger = logging.getLogger(__name__)
MaybePath = Optional[Path]


class DiskDict(Writable):
    def __init__(self, root: PathOrStr):
        root = Path(root)
        config = load_config(root)
        self.levels = config.levels
        self.hash = config.hash.build()
        assert self.hash().digest_size == sum(self.levels)

        self.root = root
        self.permissions, self.group = root_params(self.root)
        usage_folder = self.root / 'tools/usage'
        # FIXME: race condition
        create_folders(usage_folder, self.permissions, self.group)

        self.locker: Locker = config.make_locker()
        self.size_tracker: SizeTracker = config.make_size()
        self.usage_tracker: UsageTracker = config.make_usage(usage_folder)
        self.min_free_size = config.free_disk_size
        self.max_size = config.max_size

    @property
    def key_size(self):
        return sum(self.levels)

    @contextmanager
    def read(self, key: Key) -> ContextManager[MaybePath]:
        file = self._key_to_path(key)
        corrupted = False
        with self.locker.read(key):
            if file.exists():
                # TODO: deprecated
                if file.is_dir():
                    file = file / 'data'

                self.usage_tracker.update(key, file)
                try:
                    yield file
                    return
                except StorageCorruption:
                    corrupted = True

        if corrupted:
            self.delete(key)
            return

        yield None

    def read_batch(self, keys: Keys) -> Iterable[Tuple[Key, MaybeValue]]:
        for key in keys:
            with self.read(key) as value:
                yield key, value

    @contextmanager
    def write(self, key: Key, value: Value) -> ContextManager[MaybePath]:
        file = self._key_to_path(key)
        with self.locker.write(key):
            try:
                # if already stored
                if file.exists():
                    # TODO: legacy
                    if file.is_dir():
                        file = file / 'data'

                    if _is_pathlike(value):
                        match_files(value, file)
                    else:
                        with open(file, 'rb') as dst:
                            match_buffers(value, dst, context=key.hex())

                    yield file
                    return

                # make sure we can write
                if not self._writeable():
                    yield
                    return

                try:
                    # create base folders
                    create_folders(file.parent, self.permissions, self.group)
                    # populate the folder
                    if _is_pathlike(value):
                        copy_file(value, file)
                    else:
                        with open(file, 'wb') as dst:
                            shutil.copyfileobj(value, dst)

                    adjust_permissions(file, self.permissions, self.group, read_only=True)

                except BaseException as e:
                    if file.exists():
                        remove_file(file)
                    raise RuntimeError('An error occurred while copying the file') from e

                # metadata
                self.size_tracker.inc(get_size(file))
                self.usage_tracker.update(key, file)

                yield file

            except StorageCorruption:
                if file.exists():
                    remove_file(file)

    def delete(self, key: Key) -> bool:
        file = self._key_to_path(key)
        with self.locker.write(key):
            if not file.exists():
                return False

            # TODO: don't need this if no tracker is used
            if file.is_dir():
                # TODO: legacy
                size = get_size(file / 'data') if file.is_dir() else 0
                rmtree(file)
            else:
                size = get_size(file)
                remove_file(file)

            self.size_tracker.dec(size)
            self.usage_tracker.delete(key)

            return True

    def _key_to_path(self, key: Key):
        return self.root / key_to_relative(key, self.levels)

    def _writeable(self):
        result = True

        if self.min_free_size > 0:
            result = result and shutil.disk_usage(self.root).free >= self.min_free_size

        if self.max_size is not None and self.max_size < float('inf'):
            result = result and self.size_tracker.get(self.root) <= self.max_size

        return result


def _is_pathlike(x):
    return isinstance(x, (os.PathLike, str))
