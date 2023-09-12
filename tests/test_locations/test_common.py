import pickle

import pytest

from tarn import S3, DiskDict, RedisLocation


@pytest.mark.s3
@pytest.mark.redis
def test_pickleable(temp_dir, redis_hostname, s3_kwargs):
    locations = [
        DiskDict(temp_dir),
        RedisLocation(redis_hostname, prefix='abc'),
        S3(**s3_kwargs)
    ]

    for location in locations:
        raw = pickle.dumps(location)
        restored = pickle.loads(raw)
        assert location == restored
