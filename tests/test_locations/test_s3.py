import boto3
import pytest

from tarn import ReadError, HashKeyStorage
from tarn.config import StorageConfig
from tarn.location.s3 import S3


def load_text(path):
    with open(path, 'r') as file:
        return file.read()


STORAGE_ROOT = 'test'


@pytest.mark.s3
def test_storage_ssh():
    s3 = boto3.resource('s3', endpoint_url='http://127.0.0.1:8001', aws_access_key_id='admin', aws_secret_access_key='SeCrEtKeYlOCaL98327498732')
    bucket = 'kekis'
    config = StorageConfig(hash='blake2b', levels=[1, 63])
    s3.Object('kekis', f'{STORAGE_ROOT}/config.yml').put(Body=config.json())
    s3.Bucket(bucket).put_object(Key=f'{STORAGE_ROOT}/config.yml', Body=config.json())
    location = S3(s3, bucket, STORAGE_ROOT)
    storage = HashKeyStorage(location)
    key = storage.write(__file__)
    # raise ValueError(key, type(key))
    file = storage.read(lambda x: x, key)
    with pytest.raises(ReadError):
        file = storage.read(lambda x: x, b'keke' * 16)
    location.delete(key)
