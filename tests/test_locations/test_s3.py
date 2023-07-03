from hashlib import blake2b

import pytest

from tarn import HashKeyStorage, ReadError
from tarn.location.s3 import S3


def test_storage_s3(inside_ci, s3_client, bucket_name):
    location = S3(s3_client, bucket_name)
    storage = HashKeyStorage(location, algorithm=blake2b)
    key = storage.write(__file__, labels=('IRA', 'LABS'))
    key = storage.write(__file__, labels=('IRA', 'LABS', 'IRA1'))
    file = storage.read(lambda x: x, key)
    with location.write(b'123/456', b'123456', None) as v:
        pass
    with location.write(b'123/4567', __file__, None) as v:
        pass
    with location.read(key, return_labels=True) as content:
        assert sorted(content[1]) == sorted(['IRA', 'LABS', 'IRA1'])
    with pytest.raises(ReadError):
        file = storage.read(lambda x: x, b'keke' * 8)
    keys_amount = len(list(location.contents()))
    location.delete(key)
    location.delete(b'123/456')
    location.delete(b'123/4567')
    assert keys_amount - len(list(location.contents())) == 3
