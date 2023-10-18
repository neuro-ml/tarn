import datetime
import logging
import os
import random
import shutil
import string
from contextlib import contextmanager
from pathlib import Path
from typing import ContextManager, Iterable, Optional, Sequence, Tuple, Union

from ...compat import Self, copy_file, remove_file, rmtree
from ...digest import key_to_relative
from ...exceptions import CollisionError, StorageCorruption
from ...interface import Key, MaybeLabels, MaybeValue, PathOrStr, Value
from ...tools import LabelsStorage, Locker, SizeTracker, UsageTracker
from ...utils import adjust_permissions, create_folders, get_size, match_buffers, match_files
from ..interface import Location, Meta
from .config import CONFIG_NAME, StorageConfig, init_storage, load_config, root_params

logger = logging.getLogger(__name__)
MaybePath = Optional[Path]


class DiskDict(Location):
    def __init__(self, root: PathOrStr, levels: Optional[Sequence[int]] = None):
        root = Path(root)
        config = root / CONFIG_NAME
        if not config.exists():
            config = StorageConfig(levels=levels or [1, -1])
            init_storage(config, root, exist_ok=True)
        else:
            config = load_config(root)

        if levels is not None and tuple(config.levels) != tuple(levels):
            raise ValueError(f"The passed levels (levels) don't match with the ones from the config ({config.levels}).")

        self.levels = config.levels
        # TODO: deprecate
        self.hash = config.hash.build() if config.hash is not None else None

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

    def contents(self) -> Iterable[Tuple[Key, Self, Meta]]:
        tools = self.root / 'tools'
        config = self.root / 'config.yml'
        for file in self.root.glob('/'.join('*' * len(self.levels))):
            if file == config or file.is_relative_to(tools):
                continue

            key = bytes.fromhex(''.join(file.relative_to(self.root).parts))
            with self.locker.read(key):
                yield key, self, DiskDictMeta(key, self.usage_tracker, self.labels)

    @contextmanager
    def read(self, key: Key, return_labels: bool) -> ContextManager[Union[None, Value, Tuple[Value, MaybeLabels]]]:
        file = self._key_to_path(key)
        corrupted = False
        with self.locker.read(key):
            if file.exists():
                # TODO: deprecated
                if file.is_dir():
                    file = file / 'data'

                self.touch(key)
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
                self.touch(key)
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

    def touch(self, key: Key) -> bool:
        file = self._key_to_path(key)
        if not file.exists():
            return False
        self.usage_tracker.update(key)
        return True

    def _key_to_path(self, key: Key):
        assert key, 'The key must be non-empty'
        return self.root / key_to_relative(key, self.levels)

    def _writeable(self):
        result = True

        if self.min_free_size > 0:
            result = result and shutil.disk_usage(self.root).free >= self.min_free_size

        if self.max_size is not None and self.max_size < float('inf'):
            result = result and self.size_tracker.get(self.root) <= self.max_size

        return result

    def __reduce__(self):
        return type(self), (self.root, self.levels)

    def __eq__(self, other):
        return isinstance(other, DiskDict) and self.__reduce__() == other.__reduce__()


class DiskDictMeta(Meta):
    def __init__(self, key: Key, usage: UsageTracker, labels: LabelsStorage):
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
