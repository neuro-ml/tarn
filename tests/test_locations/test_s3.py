from hashlib import blake2b
import boto3
import pytest

from tarn import ReadError, HashKeyStorage
from tarn.location.s3 import S3


@pytest.mark.skip('not ready yet')
def test_storage_s3():
    s3 = boto3.client('s3', endpoint_url='http://10.0.1.2:11354')
    bucket = 'test'
    location = S3(s3, bucket)
    storage = HashKeyStorage(location, algorithm=blake2b)
    key = storage.write(__file__, labels=('IRA', 'LABS'))
    file = storage.read(lambda x: x, key)
    with location.write('123/456', b'123456', None) as v:
        pass
    with location.write('123/4567', __file__, None) as v:
        pass
    with location.read(key, return_labels=True) as content:
        assert content[1] == ['IRA', 'LABS']
    with pytest.raises(ReadError):
        file = storage.read(lambda x: x, b'keke' * 8)
    location.contents()
    location.delete(key)
