import pytest

from stash.config import StorageConfig, HashConfig


def test_config():
    a = StorageConfig(hash='sha256', levels=[1, 31])
    b = StorageConfig(hash=HashConfig(name='sha256'), levels=[1, 31])

    assert isinstance(a.hash, HashConfig)
    assert isinstance(b.hash, HashConfig)
    assert a.hash == b.hash
    assert a == b

    with pytest.raises(ValueError, match='Could not find a Locker named 1'):
        StorageConfig(hash='sha256', levels=[1, 31], locker='1').make_locker()

    StorageConfig(hash='sha256', levels=[1, 31], locker='RedisLocker')
