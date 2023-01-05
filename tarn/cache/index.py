import gzip
import logging
import os
import shutil
import tempfile
import warnings
from itertools import chain
from pathlib import Path
from typing import Any

from ..compat import rmtree, copy_file
from ..digest import key_to_relative
from ..local import Storage, DiskBase
from ..interface import Key
from ..utils import create_folders, to_read_only, match_files, adjust_permissions
from ..exceptions import StorageCorruption, ReadError
from .serializers import Serializer, SerializerError
from .compat import BadGzipFile

logger = logging.getLogger(__name__)

DATA_FOLDER = 'data'
HASH_FILENAME = 'hash.bin'
GZIP_COMPRESSION = 1


class CacheIndex(DiskBase):
    def __init__(self, root: Path, storage: Storage, serializer: Serializer):
        super().__init__(root)
        self.storage = storage
        self.serializer = serializer

    def _check_value_consistency(self, base: Path, key: Key, value: Any, context):
        check_consistency(base / HASH_FILENAME, context, check_existence=True)

    def _check_folder_consistency(self, base: Path, key: Key, folder: Path, context):
        match_files(base / HASH_FILENAME, folder / HASH_FILENAME)

    def _write(self, base: Path, key: Key, value: Any, context: Any):
        with tempfile.TemporaryDirectory() as temp_folder:
            data_folder, temp_folder = base / DATA_FOLDER, Path(temp_folder)
            create_folders(data_folder, self.permissions, self.group)

            self.serializer.save(value, temp_folder)
            self._mirror_to_storage(temp_folder, data_folder)
            self._save_meta(base, context)

    def _replicate(self, base: Path, key: Key, source: Path, context):
        # data
        destination = base / DATA_FOLDER
        shutil.copytree(source / DATA_FOLDER, destination)
        for dst in chain([destination], destination.rglob('*')):
            adjust_permissions(dst, self.permissions, self.group, read_only=not dst.is_dir())

        # meta
        copy_file(source / HASH_FILENAME, base / HASH_FILENAME)
        to_read_only(base / HASH_FILENAME, self.permissions, self.group)

    def _read(self, key, context):
        def load(base):
            try:
                return self.serializer.load(base / DATA_FOLDER, self.storage)
            except (SerializerError, ReadError):
                raise
            except Exception as e:
                raise RuntimeError(f'An error occurred while loading the cache for "{key}" at {base}') from e

        return self._read_entry(key, context, load)

    def replicate_to(self, key, context, store):
        self._read_entry(key, context, lambda base: store(key, base))

    def _read_entry(self, key, context, reader):
        base = self.root / key_to_relative(key, self.levels)
        with self.locker.read(key, base):
            if not base.exists():
                logger.info('Key %s: path not found "%s"', key, base)
                return None, False

            hash_path = base / HASH_FILENAME
            # we either have a valid folder
            if hash_path.exists():
                check_consistency(hash_path, context)
                try:
                    return reader(base), True
                except ReadError as e:
                    # couldn't find the hash - the cache is corrupted
                    logger.info('Error while reading %s: %s: %s', key, type(e).__name__, e)

        # or it is corrupted, in which case we can remove it
        with self.locker.write(key, base):
            self._cleanup_corrupted(base, key)
            return None, False

    # internal

    def _save_meta(self, base, pickled):
        hash_path = base / HASH_FILENAME
        # hash
        with gzip.GzipFile(hash_path, 'wb', compresslevel=GZIP_COMPRESSION, mtime=0) as file:
            file.write(pickled)
        to_read_only(hash_path, self.permissions, self.group)

    def _mirror_to_storage(self, source: Path, destination: Path):
        for file in source.glob('**/*'):
            target = destination / file.relative_to(source)
            if file.is_dir():
                create_folders(target, self.permissions, self.group)

            else:
                with open(target, 'w') as fd:
                    fd.write(self.storage.write(file))
                os.remove(file)
                to_read_only(target, self.permissions, self.group)

    def _cleanup_corrupted(self, folder, digest):
        message = f'Corrupted storage at {self.root} for key {digest}. Cleaning up.'
        warnings.warn(message, RuntimeWarning)
        logger.warning(message)
        rmtree(folder)


def check_consistency(hash_path, pickled, check_existence: bool = False):
    suggestion = f'You may want to delete the {hash_path.parent} folder.'
    if check_existence and not hash_path.exists():
        raise StorageCorruption(f'The pickled graph is missing. {suggestion}')
    try:
        with gzip.GzipFile(hash_path, 'rb') as file:
            dumped = file.read()
            if dumped != pickled:
                raise StorageCorruption(
                    f'The dumped and current pickle do not match at {hash_path}: {dumped} {pickled}. {suggestion}'
                )
    except BadGzipFile:
        raise StorageCorruption(f'The hash is corrupted. {suggestion}') from None
