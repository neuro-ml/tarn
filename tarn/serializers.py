import inspect
import json
import pickle
from abc import ABC, abstractmethod
from collections import defaultdict
from contextlib import suppress
from functools import partial
from gzip import GzipFile
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Callable, Dict, Iterable, Sequence, Tuple, Union

import numpy as np

from .compat import BadGzipFile, SpooledTemporaryFile
from .digest import value_to_buffer
from .exceptions import DeserializationError, SerializerError

__all__ = (
    'Serializer', 'SerializerError', 'ContentsIn', 'ContentsOut',
    'ChainSerializer', 'DictSerializer', 'NumpySerializer', 'JsonSerializer', 'PickleSerializer',
)

ContentsOut = Iterable[Tuple[str, Any]]
ContentsIn = Sequence[Tuple[str, Any]]


class Serializer(ABC):
    @abstractmethod
    def save(self, value: Any, write: Callable) -> ContentsOut:
        """ Destructures the `value` into smaller parts that can be saved to disk """

    @abstractmethod
    def load(self, contents: ContentsIn, read: Callable) -> Any:
        """ Builds the object from its `contents` """

    # TODO: legacy
    @staticmethod
    def _load_file(storage, loader: Callable, path: Path, *args, **kwargs):
        """ Useful function for loading files from storage """
        with open(path, 'r') as key:
            return storage.read(loader, key.read(), *args, **kwargs)


class ChainSerializer(Serializer):
    def __init__(self, *serializers: Serializer):
        self.serializers = serializers

    def save(self, value: Any, write: Callable) -> ContentsOut:
        for serializer in self.serializers:
            with suppress(SerializerError):
                return list(serializer.save(value, write))

        raise SerializerError(f'No serializer was able to save the value of type {type(value).__name__!r}.')

    def load(self, contents: ContentsIn, read: Callable) -> Any:
        # TODO: old style
        if isinstance(contents, (str, Path)):
            contents = [
                (str(file.relative_to(contents)), bytes.fromhex(file.read_text()))
                for file in contents.glob('**/*') if not file.is_dir()
            ]
            read = read.read

        contents = list(contents)
        for serializer in self.serializers:
            with suppress(SerializerError):
                # TODO: old style
                if list(inspect.signature(serializer.load).parameters)[0] == 'folder':
                    with TemporaryDirectory() as folder:
                        folder = Path(folder)
                        for name, value in contents:
                            (folder / name).parent.mkdir(parents=True, exist_ok=True)
                            (folder / name).write_text(value.hex())

                        storage = type('Storage', (), {'read': staticmethod(read)})
                        return serializer.load(folder, storage)

                return serializer.load(contents, read)

        raise SerializerError(f'No serializer was able to load the contents {contents}.')


class JsonSerializer(Serializer):
    def save(self, value: Any, write: Callable) -> ContentsOut:
        try:
            yield 'value.json', write(json.dumps(value, sort_keys=True).encode())
        except TypeError as e:
            raise SerializerError from e

    def load(self, contents: ContentsIn, read: Callable) -> Any:
        if len(contents) != 1:
            raise SerializerError
        path, key = contents[0]

        if path != 'value.json':
            raise SerializerError

        return read(load_json, key)


class PickleSerializer(Serializer):
    def save(self, value: Any, write: Callable) -> ContentsOut:
        try:
            yield 'value.pkl', write(pickle.dumps(value))
        except TypeError as e:
            raise SerializerError from e

    def load(self, contents: ContentsIn, read: Callable) -> Any:
        if len(contents) != 1:
            raise SerializerError
        path, key = contents[0]

        if path != 'value.pkl':
            raise SerializerError

        def loader(x):
            with value_to_buffer(x) as buffer:
                return pickle.load(buffer)

        return read(loader, key)


class NumpySerializer(Serializer):
    def __init__(self, compression: Union[int, Dict[type, int], None] = None):
        self.compression = compression

    def _choose_compression(self, value):
        if isinstance(self.compression, int) or self.compression is None:
            return self.compression

        if isinstance(self.compression, dict):
            for dtype in self.compression:
                if np.issubdtype(value.dtype, dtype):
                    return self.compression[dtype]

    def save(self, value: Any, write: Callable) -> ContentsOut:
        if not isinstance(value, (np.ndarray, np.generic)):
            raise SerializerError

        compression = self._choose_compression(value)
        # TODO: 128MB for now. move to args
        with SpooledTemporaryFile(max_size=128 * 1024 ** 2) as tmp:
            if compression is not None:
                assert isinstance(compression, int)
                with GzipFile(fileobj=tmp, mode='wb', compresslevel=compression, mtime=0) as file:
                    np.save(file, value, allow_pickle=False)

                name = 'value.npy.gz'
            else:
                np.save(tmp, value, allow_pickle=False)
                name = 'value.npy'

            tmp.seek(0)
            yield name, write(tmp)

    def load(self, contents: ContentsIn, read: Callable) -> Any:
        if len(contents) != 1:
            raise SerializerError
        path, key = contents[0]

        if path == 'value.npy':
            loader = partial(np.load, allow_pickle=False)
        elif path == 'value.npy.gz':
            def loader(x):
                with value_to_buffer(x) as buffer, GzipFile(fileobj=buffer, mode='rb') as file:
                    return np.load(file, allow_pickle=False)
        else:
            raise SerializerError

        try:
            return read(loader, key)
        except (ValueError, EOFError) as e:
            raise DeserializationError from e
        except BadGzipFile as e:
            raise SerializerError from e


class DictSerializer(Serializer):
    def __init__(self, serializer: Serializer):
        self.keys_filename = 'dict_keys.json'
        self.serializer = serializer

    def save(self, value: Any, write: Callable) -> ContentsOut:
        if not isinstance(value, dict):
            raise SerializerError

        index_to_key = {}
        for index, key in enumerate(sorted(value)):
            index_to_key[str(index)] = key
            for relative, part in self.serializer.save(value[key], write):
                yield f'{index}/{relative}', part

        yield self.keys_filename, write(json.dumps(index_to_key, sort_keys=True).encode())

    def load(self, contents: ContentsIn, read: Callable) -> Any:
        contents = dict(contents)
        if self.keys_filename not in contents:
            raise SerializerError

        index_to_key = read(load_json, contents.pop(self.keys_filename))
        groups = defaultdict(list)
        for key, value in contents.items():
            index, relative = key.split('/', 1)
            groups[index].append((relative, value))

        return {key: self.serializer.load(groups[index], read) for index, key in index_to_key.items()}


def load_json(x):
    with value_to_buffer(x) as buffer:
        return json.load(buffer)
