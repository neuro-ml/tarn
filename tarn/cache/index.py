from ..interface import PathOrStr
from ..location import DiskDict


class CacheIndex(DiskDict):
    def __init__(self, root: PathOrStr, storage, serializer):
        super().__init__(root)
        self.storage, self.serializer = storage, serializer

#     def _write(self, base: Path, key: Key, value: Any, context: Any):
#         with tempfile.TemporaryDirectory() as temp_folder:
#             data_folder, temp_folder = base / DATA_FOLDER, Path(temp_folder)
#             create_folders(data_folder, self.permissions, self.group)
#
#             self.serializer.save(value, temp_folder)
#             self._mirror_to_storage(temp_folder, data_folder)
#             self._save_meta(base, context)
#
#     def _read(self, key, context):
#         def load(base):
#             try:
#                 return self.serializer.load(base / DATA_FOLDER, self.storage)
#             except (SerializerError, ReadError):
#                 raise
#             except Exception as e:
#                 raise RuntimeError(f'An error occurred while loading the cache for "{key}" at {base}') from e
#
#         return self._read_entry(key, context, load)
#
#     def replicate_to(self, key, context, store):
#         self._read_entry(key, context, lambda base: store(key, base))
#
#     def _read_entry(self, key, context, reader):
#         base = self.root / key_to_relative(key, self.levels)
#         with self.locker.read(key, base):
#             if not base.exists():
#                 logger.info('Key %s: path not found "%s"', key, base)
#                 return None, False
#
#             hash_path = base / HASH_FILENAME
#             # we either have a valid folder
#             if hash_path.exists():
#                 check_consistency(hash_path, context)
#                 try:
#                     return reader(base), True
#                 except ReadError as e:
#                     # couldn't find the hash - the cache is corrupted
#                     logger.info('Error while reading %s: %s: %s', key, type(e).__name__, e)
#
#         # or it is corrupted, in which case we can remove it
#         with self.locker.write(key, base):
#             self._cleanup_corrupted(base, key)
#             return None, False
#
#     # internal
#
#
#
#     def _cleanup_corrupted(self, folder, digest):
#         message = f'Corrupted storage at {self.root} for key {digest}. Cleaning up.'
#         warnings.warn(message, RuntimeWarning)
#         logger.warning(message)
#         rmtree(folder)
#
#
# def check_consistency(hash_path, pickled, check_existence: bool = False):
#     suggestion = f'You may want to delete the {hash_path.parent} folder.'
#     if check_existence and not hash_path.exists():
#         raise StorageCorruption(f'The pickled graph is missing. {suggestion}')
#     try:
#         with gzip.GzipFile(hash_path, 'rb') as file:
#             dumped = file.read()
#             if dumped != pickled:
#                 raise StorageCorruption(
#                     f'The dumped and current pickle do not match at {hash_path}: {dumped} {pickled}. {suggestion}'
#                 )
#     except BadGzipFile:
#         raise StorageCorruption(f'The hash is corrupted. {suggestion}') from None
