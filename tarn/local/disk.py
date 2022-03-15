import logging
import shutil
from pathlib import Path
from typing import Any, Tuple, Callable

from ..digest import digest_file
from ..interface import Key
from ..local import DiskBase
from ..utils import to_read_only, copy_file, match_files

FILENAME = 'data'
logger = logging.getLogger(__name__)


class Disk(DiskBase):
    def _read(self, key, context: Callable) -> Tuple[Any, bool]:
        return self._read_entry(key, lambda base: context(base / FILENAME))

    def replicate_to(self, key, context, store: Callable[[Key, Path], Any]):
        self._read_entry(key, lambda base: store(key, base))

    def _check_value_consistency(self, base: Path, key: Key, value: Any, context):
        match_files(value, base / FILENAME)

    def _check_folder_consistency(self, base: Path, key: Key, folder: Path, context):
        match_files(folder / FILENAME, base / FILENAME)

    def _write(self, base: Path, key: Key, value: Path, context):
        value = Path(value)
        assert value.is_file(), value

        file = base / FILENAME
        copy_file(value, file)
        # make file read-only
        to_read_only(file, self.permissions, self.group)
        digest = digest_file(file, self.algorithm)
        if digest != key:
            shutil.rmtree(base)
            raise ValueError(f'The stored file has a wrong hash: expected {key} got {digest}. '
                             'The file was most likely corrupted while copying')

    def _replicate(self, base: Path, key: Key, source: Path, context):
        self._write(base, key, source / FILENAME, context)

    def _read_entry(self, key, reader) -> Tuple[Any, bool]:
        base = self._key_to_base(key)

        with self.locker.read(key, base):
            if not base.exists():
                return None, False

            return reader(base), True
