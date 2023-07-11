import datetime
import logging
import os
import random
import shutil
import string
from contextlib import contextmanager
from pathlib import Path
from typing import ContextManager, Iterable, Optional, Tuple, Union

from ..compat import Self, copy_file, remove_file, rmtree
from ..config import load_config, root_params
from ..digest import key_to_relative
from ..exceptions import CollisionError, StorageCorruption
from ..interface import Key, Keys, MaybeLabels, MaybeValue, PathOrStr, Value
from ..tools import Locker, SizeTracker, UsageTracker
from ..tools.labels import LabelsStorage
from ..utils import adjust_permissions, create_folders, get_size, match_buffers, match_files
from .interface import Meta, Writable

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
        self.tmp = root / '.tmp'
        create_folders(self.tmp, self.permissions, self.group)

        # TODO: create these folders only if needed
        usage_folder = self.root / 'tools/usage'
        labels_folder = self.root / 'tools/labels'
        create_folders(usage_folder, self.permissions, self.group)
        create_folders(labels_folder, self.permissions, self.group)

        self.locker: Locker = config.make_locker()
        self.size_tracker: SizeTracker = config.make_size()
        self.usage_tracker: UsageTracker = config.make_usage(usage_folder)
        self.labels: LabelsStorage = config.make_labels(labels_folder)
        self.min_free_size = config.free_disk_size
        self.max_size = config.max_size

    @property
    def key_size(self):
        return sum(self.levels)

    def contents(self) -> Iterable[Tuple[Key, Self, Meta]]:
        tools = self.root / 'tools'
        config = self.root / 'config.yml'
        for file in self.root.glob('/'.join('*' * len(self.levels))):
            if file == config or file.is_relative_to(tools):
                continue

            key = bytes.fromhex(''.join(file.relative_to(self.root).parts))
            with self.locker.read(key):
                yield key, self, str(self.root)

    @contextmanager
    def read(self, key: Key, return_labels: bool) -> ContextManager[Union[None, Value, Tuple[Value, MaybeLabels]]]:
        file = self._key_to_path(key)
        corrupted = False
        with self.locker.read(key):
            if file.exists():
                # TODO: deprecated
                if file.is_dir():
                    file = file / 'data'

                self.usage_tracker.update(key)
                try:
                    if return_labels:
                        yield file, self.labels.get(key)
                    else:
                        yield file
                    return
                except StorageCorruption:
                    corrupted = True

        if corrupted:
            self.delete(key)
            return

        yield None

    def read_batch(self, keys: Keys) -> Iterable[Tuple[Key, Union[Value, MaybeLabels]]]:
        for key in keys:
            with self.read(key, True) as value:
                yield key, value

    @contextmanager
    def write(self, key: Key, value: Value, labels: MaybeLabels) -> ContextManager[MaybeValue]:
        file = self._key_to_path(key)
        with self.locker.write(key):
            try:
                # if already stored
                if file.exists():
                    # TODO: legacy
                    if file.is_dir():
                        file = file / 'data'

                    _match(value, file, key)
                    yield file
                    self.labels.update(key, labels)
                    return

                # make sure we can write
                if not self._writeable():
                    yield
                    return

                tmp = self.tmp / (key.hex() + ''.join(random.choices(string.ascii_lowercase, k=8)))
                try:
                    # create base folders
                    create_folders(file.parent, self.permissions, self.group)

                    # write to a temp location
                    if _is_pathlike(value):
                        copy_file(value, tmp)
                    else:
                        with open(tmp, 'wb') as dst:
                            shutil.copyfileobj(value, dst)
                    # permissions
                    adjust_permissions(tmp, self.permissions, self.group, read_only=True)

                    # move the file to the right location
                    shutil.move(tmp, file)

                except BaseException as e:
                    if file.exists():
                        remove_file(file)
                    raise RuntimeError('An error occurred while copying the file') from e

                finally:
                    if tmp.exists():
                        remove_file(tmp)

                # metadata
                self.size_tracker.inc(get_size(file))
                self.usage_tracker.update(key)
                self.labels.update(key, labels)

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
            self.labels.delete(key)

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


class DiskDictMeta(Meta):
    def __init__(self, key, usage, labels):
        self._key, self._usage, self._labels = key, usage, labels

    @property
    def last_used(self) -> Optional[datetime.datetime]:
        return self._usage.get(self._key)

    @property
    def labels(self) -> MaybeLabels:
        return self._labels.get(self._key)


def _is_pathlike(x):
    return isinstance(x, (os.PathLike, str))


def _match(value, file, key):
    try:
        if _is_pathlike(value):
            match_files(value, file)
        else:
            with open(file, 'rb') as dst:
                match_buffers(value, dst, context=key.hex())
    except ValueError as e:
        raise CollisionError(f"Written value and the new one doesn't match: {key}") from e
