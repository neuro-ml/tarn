import logging
import shutil
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Tuple

from tqdm import tqdm

from ..config import root_params, load_config
from ..digest import key_to_relative
from ..interface import LocalStorage, Key
from ..tools import Locker, SizeTracker, UsageTracker
from ..utils import get_size, create_folders, PathLike

logger = logging.getLogger(__name__)


class DiskBase(LocalStorage, ABC):
    def __init__(self, root: PathLike):
        config = load_config(root)
        super().__init__(config.hash, config.levels)
        self.root = Path(root)
        self.permissions, self.group = root_params(self.root)

        self.locker: Locker = config.make_locker()
        self.size_tracker: SizeTracker = config.make_size()
        self.usage_tracker: UsageTracker = config.make_usage()
        self.min_free_size = config.free_disk_size
        self.max_size = config.max_size

    def read(self, key, context: Any) -> Tuple[Any, bool]:
        value, success = self._read(key, context)
        if success:
            self.usage_tracker.update(key, self._key_to_base(key))
        return value, success

    def write(self, key, value: Any, context: Any) -> bool:
        return self._protected_write(key, value, context, self._check_value_consistency, self._write)

    def delete(self, key, context: Any) -> bool:
        base = self._key_to_base(key)
        with self.locker.write(key, base):
            if not base.exists():
                return False

            # TODO: don't need this if no tracker is used
            size = self._get_size(base)
            shutil.rmtree(base)
            self.size_tracker.dec(size, self.root)
            self.usage_tracker.delete(key, base)

            return True

    def contains(self, key: Key, context):
        """ This is not safe, but it's fast. """
        base = self._key_to_base(key)
        with self.locker.read(key, base):
            return base.exists()

    def replicate_from(self, key, source: Path, context: Any) -> bool:
        return self._protected_write(key, source, context, self._check_folder_consistency, self._replicate)

    def actualize(self, verbose: bool):
        """ Useful for migration between locking mechanisms. """
        # TODO: need a global lock
        size = 0
        bar = tqdm(self.root.glob(f'**/*'), disable=not verbose)
        for file in bar:
            if file.is_dir():
                continue

            bar.set_description(str(file.parent.relative_to(self.root)))
            assert file.is_file()
            size += get_size(file)

        self.size_tracker.set(size, self.root)

    # internal

    def _key_to_base(self, key: Key):
        return self.root / key_to_relative(key, self.levels)

    def _writeable(self):
        result = True

        if self.min_free_size > 0:
            result = result and shutil.disk_usage(self.root).free >= self.min_free_size

        if self.max_size is not None and self.max_size < float('inf'):
            result = result and self.size_tracker.get(self.root) <= self.max_size

        return result

    @staticmethod
    def _get_size(base: Path):
        return sum(get_size(file) for file in base.glob('**/*') if file.is_file())

    def _protected_write(self, key: Key, value: Any, context, check_consistency, write) -> bool:
        base = self._key_to_base(key)
        with self.locker.write(key, base):
            # if already stored
            if base.exists():
                check_consistency(base, key, value, context)
                return True

            # make sure we can write
            if not self._writeable():
                return False

            try:
                # create base folder
                create_folders(base, self.permissions, self.group)
                # populate the folder
                write(base, key, value, context)

            except BaseException as e:
                if base.exists():
                    shutil.rmtree(base)
                raise RuntimeError('An error occurred while copying the file') from e

            # metadata
            self.size_tracker.inc(self._get_size(base), self.root)
            self.usage_tracker.update(key, base)

            return True

    @abstractmethod
    def _check_value_consistency(self, base: Path, key: Key, value: Any, context):
        """ Make sure that the written value is the same as the stored one """

    @abstractmethod
    def _check_folder_consistency(self, base: Path, key: Key, folder: Path, context):
        """ Make sure that the replicated folder is the same as the stored one """

    @abstractmethod
    def _read(self, key: Key, context):
        """ Read an entry given the ``key`` """

    @abstractmethod
    def _write(self, base: Path, key: Key, value: Any, context):
        """ Write a ``value`` given the ``key`` """

    @abstractmethod
    def _replicate(self, base: Path, key: Key, source: Path, context):
        """ Copy the content from ``source`` to ``base`` """
