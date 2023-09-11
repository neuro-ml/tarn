import pickle

from tarn import DiskDict, RedisLocation


def test_pickleable(temp_dir, redis_hostname):
    locations = [
        DiskDict(temp_dir),
        RedisLocation(redis_hostname, prefix='abc'),
    ]

    for location in locations:
        raw = pickle.dumps(location)
        restored = pickle.loads(raw)
        assert location == restored
