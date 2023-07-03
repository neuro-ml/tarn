from hashlib import blake2b

import pytest
import redis

from tarn import HashKeyStorage, ReadError
from tarn.location.redis import RedisLocation


def test_storage_redis():
    redis_instance = redis.Redis()
    location = RedisLocation(redis_instance, prefix='_')
    storage = HashKeyStorage(location, algorithm=blake2b)
    key = storage.write(__file__, labels=('IRA', 'LABS'))
    key = storage.write(__file__, labels=('IRA1', 'LABS'))
    file = storage.read(lambda x: x, key)
    with location.write(b'123/456', b'123456', None) as v:
        pass
    with location.read(key, return_labels=True) as content:
        assert sorted(content[1]) == sorted(['IRA', 'LABS', 'IRA1'])
    with pytest.raises(ReadError):
        file = storage.read(lambda x: x, b'keke' * 8)
    keys_amount = len(list(location.contents()))
    location.delete(key)
    location.delete(b'123/456')
    assert keys_amount - len(list(location.contents())) == 2
