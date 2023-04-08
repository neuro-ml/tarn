import os
from contextlib import contextmanager
from pathlib import Path
from typing import AnyStr, Sequence, Type

from .compat import HashAlgorithm
from .interface import Value


def digest_file(path, algorithm, block_size=2 ** 20):
    return digest_value(path, algorithm, block_size).hex()


def digest_value(value: Value, algorithm: Type[HashAlgorithm], block_size: int = 2 ** 20) -> bytes:
    with _value_to_buffer(value) as buffer:
        hasher = algorithm()
        while True:
            chunk = buffer.read(block_size)
            if not chunk:
                break
            hasher.update(chunk)

        return hasher.digest()


def key_to_relative(key: AnyStr, levels: Sequence[int]):
    if isinstance(key, bytes):
        key = key.hex()

    # TODO: too expensive?
    assert len(key) == get_digest_size(levels, string=True), (len(key), get_digest_size(levels, string=True))

    parts = []
    start = 0
    for level in levels:
        stop = start + level * 2
        parts.append(key[start:stop])
        start = stop

    return Path(*parts)


def get_digest_size(levels, string: bool):
    size = sum(levels)
    if string:
        size *= 2
    return size


@contextmanager
def _value_to_buffer(value: Value):
    if isinstance(value, (str, os.PathLike)):
        with open(value, 'rb') as file:
            yield file

    else:
        yield value
