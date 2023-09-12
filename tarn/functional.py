import hashlib
import inspect
import os
from functools import wraps
from pathlib import Path
from typing import Collection, Type, Union

from .location import DiskDict
from .compat import HashAlgorithm
from .pool import PickleKeyStorage
from .pool.hash_key import HashKeyStorage, LocationsLike
from .serializers import DefaultSerializer, Serializer


def smart_cache(
        index: LocationsLike,
        storage: Union[HashKeyStorage, LocationsLike, None] = None,
        serializer: Serializer = DefaultSerializer,
        algorithm: Union[Type[HashAlgorithm], str, None] = None,
        stable_objects: Collection = (),
        unstable_objects: Collection = (),
        unstable_modules: Collection = ()
):
    if storage is None:
        assert isinstance(index, (str, os.PathLike))
        algo = algorithm
        if algo is None:
            algo = 'sha256'
        if isinstance(algo, str):
            algo = getattr(hashlib, algo)

        root = Path(index)
        index, storage = root / 'index', root / 'storage'
        index, storage = DiskDict(index), HashKeyStorage(storage, algorithm=algo)

    pool = PickleKeyStorage(
        index,
        storage,
        serializer,
        algorithm=algorithm,
        stable_objects=stable_objects,
        unstable_objects=unstable_objects,
        unstable_modules=unstable_modules
    )

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
