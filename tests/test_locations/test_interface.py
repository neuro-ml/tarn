from pathlib import Path

import pytest

from tarn import DiskDict, StorageCorruption, Writable, Levels, Fanout, Level


def _mkdir(x):
    x.mkdir(parents=True, exist_ok=True)
    (x / 'config.yml').write_text('{levels: [1, 31], hash: sha256}')
    return x


@pytest.fixture(params=[
    lambda x: DiskDict(_mkdir(x)),
    lambda x: Levels(DiskDict(_mkdir(x))),
    lambda x: Fanout(DiskDict(_mkdir(x))),
    lambda x: Levels(Fanout(DiskDict(_mkdir(x)))),
    lambda x: Fanout(Levels(DiskDict(_mkdir(x)))),
    # multiple shards / levels
    lambda x: Levels(
        Level(DiskDict(_mkdir(x / 'one')), write=False),
        DiskDict(_mkdir(x / 'two'))
    ),
    lambda x: Fanout(DiskDict(_mkdir(x / 'one')), DiskDict(_mkdir(x / 'two'))),
])
def location(request, temp_dir) -> Writable:
    return request.param(temp_dir)


def test_errors_propagation(location):
    key = b'0' * location.key_size
    with pytest.raises(ZeroDivisionError):
        with location.write(key, __file__):
            raise ZeroDivisionError

    # existing key
    with pytest.raises(ZeroDivisionError):
        with location.read(key):
            raise ZeroDivisionError

    # missing key
    with pytest.raises(ZeroDivisionError):
        with location.read(b'1' * location.key_size):
            raise ZeroDivisionError


def test_corrupted_read(location):
    key, value = b'0' * location.key_size, Path(__file__)

    with location.write(key, value):
        pass

    with location.read(key) as result:
        assert result.read_bytes() == value.read_bytes()
        raise StorageCorruption

    with location.read(key) as result:
        assert result is None


def test_corrupted_write(location):
    key, value = b'0' * location.key_size, Path(__file__)

    with location.write(key, value) as result:
        assert result.read_bytes() == value.read_bytes()
        raise StorageCorruption

    with location.read(key) as result:
        assert result is None
