import boto3
import pytest

from tarn import ReadError, HashKeyStorage
from tarn.location.s3 import S3


@pytest.mark.skip(reason='not ready yet')
def test_storage_s3():
    s3 = boto3.client('s3', endpoint_url='http://127.0.0.1:8001', aws_access_key_id='admin', aws_secret_access_key='SeCrEtKeYlOCaL98327498732')
    bucket = 'kekis'
    location = S3(s3, bucket)
    storage = HashKeyStorage(location)
    key = storage.write(__file__, labels=('IRA', 'LABS'))
    file = storage.read(lambda x: x, key)
    with pytest.raises(ReadError):
        file = storage.read(lambda x: x, b'keke' * 8)
    location.delete(key)
