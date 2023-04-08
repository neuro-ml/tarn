import json
import pickle
from abc import ABC, abstractmethod
from contextlib import suppress
from functools import partial
from gzip import GzipFile
from pathlib import Path
from typing import Callable, Dict, Union

import numpy as np

from .compat import rmtree
from .exceptions import ReadError
from .pool import HashKeyStorage

__all__ = (
    'Serializer', 'SerializerError', 'ChainSerializer', 'DictSerializer',
    'NumpySerializer', 'JsonSerializer', 'PickleSerializer',
)


class SerializerError(Exception):
    pass


class Serializer(ABC):
    @abstractmethod
    def save(self, value, folder: Path):
        """ Saves the ``value`` to ``folder`` """

    @abstractmethod
    def load(self, folder: Path, storage: HashKeyStorage):
        """ Loads the value from ``folder`` """

    @staticmethod
    def _load_file(storage: HashKeyStorage, loader: Callable, path: Path, *args, **kwargs):
        """ Useful function for loading files from storage """
        with open(path, 'r') as key:
            return storage.read(loader, key.read(), *args, **kwargs)


class ChainSerializer(Serializer):
    def __init__(self, *serializers: Serializer):
        self.serializers = serializers

    def save(self, value, folder: Path):
        for serializer in self.serializers:
            with suppress(SerializerError):
                return serializer.save(value, folder)

        raise SerializerError(f'No serializer was able to save to {folder}.')

    def load(self, folder: Path, storage: HashKeyStorage):
        for serializer in self.serializers:
            with suppress(SerializerError):
                return serializer.load(folder, storage)

        raise SerializerError(f'No serializer was able to load from {folder}.')


class JsonSerializer(Serializer):
    def save(self, value, folder: Path):
        try:
            value = json.dumps(value)
        except TypeError as e:
            raise SerializerError from e

        with open(folder / 'value.json', 'w') as file:
            file.write(value)

    def load(self, folder: Path, storage: HashKeyStorage):
        paths = list(folder.iterdir())
        if len(paths) != 1:
            raise SerializerError

        path, = paths
        if path.name != 'value.json':
            raise SerializerError

        def loader(x):
            with open(x, 'r') as file:
                return json.load(file)

        return self._load_file(storage, loader, folder / 'value.json')


class PickleSerializer(Serializer):
    def save(self, value, folder):
        try:
            value = pickle.dumps(value)
        except TypeError as e:
            raise SerializerError from e

        with open(folder / 'value.pkl', 'wb') as file:
            file.write(value)

    def load(self, folder: Path, storage: HashKeyStorage):
        paths = list(folder.iterdir())
        if len(paths) != 1:
            raise SerializerError

        path, = paths
        if path.name != 'value.pkl':
            raise SerializerError

        def loader(x):
            with open(x, 'rb') as file:
                return pickle.load(file)

        return self._load_file(storage, loader, folder / 'value.pkl')


class NumpySerializer(Serializer):
    def __init__(self, compression: Union[int, Dict[type, int]] = None):
        self.compression = compression

    def _choose_compression(self, value):
        if isinstance(self.compression, int) or self.compression is None:
            return self.compression

        if isinstance(self.compression, dict):
            for dtype in self.compression:
                if np.issubdtype(value.dtype, dtype):
                    return self.compression[dtype]

    def save(self, value, folder: Path):
        if not isinstance(value, (np.ndarray, np.generic)):
            raise SerializerError

        compression = self._choose_compression(value)
        if compression is not None:
            assert isinstance(compression, int)
            with GzipFile(folder / 'value.npy.gz', 'wb', compresslevel=compression, mtime=0) as file:
                np.save(file, value, allow_pickle=False)

        else:
            np.save(folder / 'value.npy', value, allow_pickle=False)

    def load(self, folder: Path, storage: HashKeyStorage):
        paths = list(folder.iterdir())
        if len(paths) != 1:
            raise SerializerError

        path, = paths
        if path.name == 'value.npy':
            loader = partial(np.load, allow_pickle=False)
        elif path.name == 'value.npy.gz':
            def loader(x):
                with GzipFile(x, 'rb') as file:
                    return np.load(file, allow_pickle=False)
        else:
            raise SerializerError

        try:
            return self._load_file(storage, loader, path)
        except ValueError as e:
            raise ReadError from e


class DictSerializer(Serializer):
    def __init__(self, serializer: Serializer):
        self.keys_filename = 'dict_keys.json'
        self.serializer = serializer

    def save(self, data: dict, folder: Path):
        if not isinstance(data, dict):
            raise SerializerError

        try:
            keys_to_folder = {}
            for index, (key, value) in enumerate(data.items()):
                keys_to_folder[index] = key
                sub_folder = folder / str(index)
                sub_folder.mkdir()
                self.serializer.save(value, sub_folder)

        except SerializerError:
            # remove the partially saved object
            for sub_folder in folder.iterdir():
                rmtree(sub_folder)

            raise

        with open(folder / self.keys_filename, 'w+') as f:
            json.dump(keys_to_folder, f)

    def load(self, folder: Path, storage: HashKeyStorage):
        keys = folder / self.keys_filename
        if not keys.exists():
            raise SerializerError

        def loader(x):
            with open(x, 'r') as f:
                return json.load(f)

        keys_map = self._load_file(storage, loader, keys)
        data = {}
        for sub_folder, key in keys_map.items():
            data[key] = self.serializer.load(folder / sub_folder, storage)
        return data
