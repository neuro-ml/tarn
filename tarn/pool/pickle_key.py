import json
import logging
from contextlib import contextmanager
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, NamedTuple, Optional, Sequence, Type, Union

from ..compat import HashAlgorithm
from ..exceptions import ReadError, StorageCorruption, WriteError
from ..interface import Key, PathOrStr
from ..location import Level, Location
from ..pickler import PREVIOUS_VERSIONS, dumps
from ..serializers import Serializer, SerializerError
from .hash_key import HashKeyStorage, LocationsLike, resolve_location

logger = logging.getLogger(__name__)

LocationLike = Union[Level, Sequence[Location], Location, Sequence[PathOrStr], PathOrStr]
ProxyKey = Any


class _PreparedKey(NamedTuple):
    digest: bytes
    raw: Any
    pickled: bytes


class PickleKeyStorage:
    def __init__(self, index: LocationsLike, storage: Union[HashKeyStorage, LocationsLike], serializer: Serializer,
                 algorithm: Type[HashAlgorithm] = None):
        index = resolve_location(index)
        if not isinstance(storage, HashKeyStorage):
            storage = HashKeyStorage(storage)
        if algorithm is None:
            assert index.hash is not None
            algorithm = index.hash
        elif index.hash is not None:
            assert algorithm == index.hash

        self.index = index
        self.storage = storage
        self.serializer = serializer
        self.algorithm = algorithm

    def prepare(self, key: ProxyKey) -> _PreparedKey:
        pickled, digest = _key_to_digest(self.algorithm, key)
        return _PreparedKey(digest, key, pickled)

    def read(self, key: ProxyKey, *, error: bool = True):
        if not isinstance(key, _PreparedKey):
            key = self.prepare(key)

        value, exists = self._read(key)
        if not error:
            return value, bool(exists)

        if exists:
            return value
        if exists is None:
            raise WriteError(f"The key {key.digest.hex()} couldn't be written to any storage")
        raise ReadError(f'Key {key.digest.hex()} is not found')

    def write(self, key: ProxyKey, value: Any, *, error: bool = True) -> Optional[Key]:
        if not isinstance(key, _PreparedKey):
            key = self.prepare(key)

        digest = key.digest
        logger.info('Serializing %s', digest)
        mapping = {}
        with TemporaryDirectory() as temp_folder:
            temp_folder = Path(temp_folder)
            self.serializer.save(value, temp_folder)

            for file in temp_folder.glob('**/*'):
                if file.is_dir():
                    continue

                relative = str(file.relative_to(temp_folder))
                assert relative not in mapping
                mapping[relative] = self.storage.write(file).hex()

        logger.info('Saving to index %s', digest)
        with self.index.write(digest, BytesIO(json.dumps(mapping).encode())) as written:
            if written is None:
                if error:
                    raise WriteError('The index could not be written to any storage')
                return None

        return digest

    def _read_for_digest(self, digest):
        with self.index.read(digest) as index:
            if index is None:
                return None, False

            with _unpack_mapping(index) as folder:
                try:
                    return self.serializer.load(folder, self.storage), True
                except ReadError as e:
                    raise StorageCorruption from e

                except SerializerError:
                    raise
                except Exception as e:
                    raise RuntimeError(f'An error occurred while loading the cache for "{digest.hex()}"') from e

        return None, False

    def _read(self, key: _PreparedKey):
        digest = key.digest
        value, exists = self._read_for_digest(digest)
        if exists:
            logger.info('Key %s found', digest)
            return value, True

        # the cache is empty, but we can try and restore it from an older version
        for version in reversed(PREVIOUS_VERSIONS):
            _, local_digest = _key_to_digest(self.algorithm, key.raw, version)
            value, exists = self._read_for_digest(digest)
            if exists:
                logger.info('Key %s found in previous version (%d). Updating', digest, version)
                # and store it for faster access next time
                self.write(key, value, error=False)
                return value, True

        logger.info('Key %s not found', digest)
        return None, False


def _key_to_digest(algorithm, key, version=None):
    pickled = dumps(key, version=version)
    digest = algorithm(pickled).digest()
    return pickled, digest


@contextmanager
def _unpack_mapping(path: Path):
    if path.is_file():
        with open(path, 'r') as file:
            try:
                mapping = json.load(file)
            except json.JSONDecodeError as e:
                raise StorageCorruption from e

        with TemporaryDirectory() as temp:
            temp = Path(temp)
            for relative, content in mapping.items():
                (temp / relative).parent.mkdir(parents=True, exist_ok=True)
                (temp / relative).write_text(content)

            yield temp

    else:
        # TODO: warn
        yield path / 'data'
