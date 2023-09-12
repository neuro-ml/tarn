from io import BytesIO

import pytest

from tarn import Fanout, Levels, RedisLocation, Small


@pytest.mark.redis
def test_buffer_exhaustion(redis_hostname):
    big = RedisLocation(redis_hostname)
    small = Small(RedisLocation(redis_hostname), 5)
    levels = Levels(small, big)
    fanout = Fanout(small, big)
    key = b'1234567'
    value = BytesIO(key)
    with levels.write(key, value, None) as v:
        assert v is not None, v
    with levels.read(key, False) as v:
        v = v.read()
        assert v == key, v
    levels.delete(key)

    value = BytesIO(key)
    with fanout.write(key, value, None) as v:
        assert v is not None, v
    with fanout.read(key, False) as v:
        v = v.read()
        assert v == key, v
    fanout.delete(key)
