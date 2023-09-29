from pathlib import Path

import pytest

from tarn import DiskDict, Fanout, Level, Levels, Location, StorageCorruption


def _mkdir(x):
    x.mkdir(parents=True, exist_ok=True)
    (x / 'config.yml').write_text('{levels: [1, -1], hash: sha256}')
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
def location(request, temp_dir) -> Location:
    return request.param(temp_dir)


def test_errors_propagation(location):
    key = b'0' * 100
    with pytest.raises(ZeroDivisionError):
        with location.write(key, __file__, None):
            raise ZeroDivisionError

    # existing key
    with pytest.raises(ZeroDivisionError):
        with location.read(key, False):
            raise ZeroDivisionError

    # missing key
    with pytest.raises(ZeroDivisionError):
        with location.read(b'1' * 100, False):
            raise ZeroDivisionError


def test_corrupted_read(location):
    key, value = b'0' * 100, Path(__file__)

    with location.write(key, value, None):
        pass

    with location.read(key, False) as result:
        assert result.read_bytes() == value.read_bytes()
        raise StorageCorruption

    with location.read(key, False) as result:
        assert result is None


def test_corrupted_write(location):
    key, value = b'0' * 100, Path(__file__)

    with location.write(key, value, None) as result:
        assert result.read_bytes() == value.read_bytes()
        raise StorageCorruption

    with location.read(key, False) as result:
        assert result is None
