from hashlib import blake2b
from pathlib import Path

import pytest

from tarn import HashKeyStorage, ReadError
from tarn.location.s3 import S3


@pytest.mark.s3
def test_storage_s3(s3_client, bucket_name):
    location = S3(s3_client, bucket_name)
    storage = HashKeyStorage(location, algorithm=blake2b)
    key = storage.write(Path(__file__), labels=('IRA', 'LABS'))
    key = storage.write(Path(__file__), labels=('IRA', 'LABS', 'IRA1'))
    file = storage.read(lambda x: x, key)
    with location.write(b'123/456', b'123456', None) as v:
        pass
    with location.write(b'123/4567', Path(__file__), None) as v:
        pass
    with location.read(key, return_labels=True) as content:
        assert sorted(content[1]) == sorted(['IRA', 'LABS', 'IRA1'])
    with location.read(b'123/456', return_labels=False) as content:
        assert content.read() == b'123456'
    for k, v in location.read_batch((b'123/456',)):
        pass
    with pytest.raises(ReadError):
        file = storage.read(lambda x: x, b'keke' * 8)
    contents = list(location.contents())
    keys_amount = len(contents)
    location.delete(key)
    location.delete(b'123/456')
    location.delete(b'123/4567')
    assert keys_amount - len(list(location.contents())) == 3
