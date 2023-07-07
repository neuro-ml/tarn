import json
import logging
import os
from io import BytesIO
from pathlib import Path
from typing import Any, NamedTuple, Optional, Sequence, Type, Union

from ..compat import HashAlgorithm
from ..exceptions import CollisionError, DeserializationError, ReadError, StorageCorruption, WriteError
from ..interface import Key, MaybeLabels, PathOrStr, Value
from ..location import Level, Location
from ..pickler import PREVIOUS_VERSIONS, dumps
from ..serializers import Serializer, SerializerError
from ..utils import value_to_buffer
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
                 algorithm: Optional[Type[HashAlgorithm]] = None):
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

    def write(self, key: ProxyKey, value: Any, *, error: bool = True, labels: MaybeLabels = None) -> Optional[Key]:
        if not isinstance(key, _PreparedKey):
            key = self.prepare(key)

        digest = key.digest
        logger.info('Serializing %s', digest)
        mapping = dict(self.serializer.save(value, lambda v: self.storage.write(v, labels=labels).hex()))

        # we want a reproducible mapping each time
        logger.info('Saving to index %s', digest)
        try:
            with self.index.write(digest, BytesIO(json.dumps(mapping, sort_keys=True).encode()), labels=None) as written:
                if written is None:
                    if error:
                        raise WriteError('The index could not be written to any storage')
                    return None
        except CollisionError as e:
            with self.index.read(digest, return_labels=False) as v:
                raise CollisionError(f'Old mapping: {v.read()}. New mapping: {mapping}') from e
        return digest

    def _read_for_digest(self, digest):
        with self.index.read(digest, False) as index:
            if index is None:
                return None, False

            contents = list(_unpack_mapping(index))
            try:
                return self.serializer.load(contents, self.storage.read), True
            # either the data is corrupted or missing
            except (DeserializationError, ReadError) as e:
                raise StorageCorruption from e
            except SerializerError as e:
                raise SerializerError(f'Could not deserialize the data from key {digest.hex()}') from e
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
            value, exists = self._read_for_digest(local_digest)
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


def _unpack_mapping(value: Value):
    if isinstance(value, (str, os.PathLike)):
        value = Path(value)
    # TODO: legacy
    if isinstance(value, Path) and not value.is_file():
        for file in value.glob('**/*'):
            if file.is_dir():
                continue

            relative = str(file.relative_to(value))
            yield relative, bytes.fromhex(file.read_text())

    else:
        with value_to_buffer(value) as buffer:
            try:
                mapping = json.load(buffer)
            except json.JSONDecodeError as e:
                raise StorageCorruption from e

            for k, v in mapping.items():
                yield k, bytes.fromhex(v)
