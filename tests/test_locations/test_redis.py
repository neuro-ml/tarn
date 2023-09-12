from hashlib import blake2b
from pathlib import Path

import cloudpickle
import pytest
import redis

from tarn import HashKeyStorage, ReadError, RedisLocation


@pytest.mark.redis
def test_storage_redis(redis_hostname):
    redis_instance = redis.Redis(redis_hostname)
    location = RedisLocation(redis_instance, prefix='___test___')
    storage = HashKeyStorage(location, algorithm=blake2b)
    key = storage.write(Path(__file__), labels=('IRA', 'LABS'))
    key = storage.write(Path(__file__), labels=('IRA1', 'LABS'))
    file = storage.read(lambda x: x, key)
    with location.write(b'123456', b'123456', None) as v:
        pass
    with location.read(b'123456', return_labels=False) as v:
        assert v.read() == b'123456'
    with location.read(key, return_labels=True) as content:
        assert sorted(content[1]) == sorted(['IRA', 'LABS', 'IRA1'])
    for k, v in location.read_batch((b'123456', )):
        pass
    with pytest.raises(ReadError):
        file = storage.read(lambda x: x, b'keke' * 8)
    contents = list(location.contents())
    for content in contents:
        str(content[-1]) != 'None'
    keys_amount = len(contents)
    location.delete(key)
    location.delete(b'123456')
    assert keys_amount - len(list(location.contents())) == 2


@pytest.mark.redis
def test_redis_pickle(redis_hostname):
    redis = RedisLocation(redis_hostname, prefix=b':')
    x = cloudpickle.loads(cloudpickle.dumps(redis))
    xx = cloudpickle.loads(cloudpickle.dumps(x))

    assert x.redis.get_connection_kwargs() == xx.redis.get_connection_kwargs()
    assert x.prefix == xx.prefix == b':'
