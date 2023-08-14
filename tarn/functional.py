import hashlib
import inspect
import os
from functools import wraps
from pathlib import Path
from typing import Union, Type

from . import DiskDict
from .compat import HashAlgorithm
from .pool import PickleKeyStorage
from .pool.hash_key import LocationsLike, HashKeyStorage
from .serializers import Serializer


def smart_cache(index: LocationsLike, storage: Union[HashKeyStorage, LocationsLike, None] = None,
                serializer: Serializer = None, algorithm: Union[Type[HashAlgorithm], str, None] = None):
    if storage is None:
        assert isinstance(index, (str, os.PathLike))
        algo = algorithm
        if algo is None:
            algo = 'sha256'
        if isinstance(algo, str):
            algo = getattr(hashlib, algo)

        levels = 1, algo().digest_size
        root = Path(index)
        index, storage = root / 'index', root / 'storage'
        if not index.exists():
            DiskDict.create(index, levels)
        if not storage.exists():
            DiskDict.create(storage, levels)
        index, storage = DiskDict(index), HashKeyStorage(storage, algorithm=algo)

    pool = PickleKeyStorage(index, storage, serializer, algorithm=algorithm)

    def decorator(func):
        signature = inspect.signature(func)

        @wraps(func)
        def wrapper(*args, **kwargs):
            bound = signature.bind(*args, **kwargs)
            key = pool.prepare((func, *bound.arguments.values()))
            value, success = pool.read(key, error=False)
            if success:
                return value

            value = func(*args, **kwargs)
            pool.write(key, value, error=False)
            return value

        return wrapper

    return decorator
