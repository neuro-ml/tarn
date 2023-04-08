from pathlib import Path

import pytest

from tarn import Nginx, ReadError, HashKeyStorage


def load_text(path):
    with open(path, 'r') as file:
        return file.read()


STORAGE_ROOT = '/tmp/http'
STORAGE_URL = 'http://localhost:8765'


@pytest.mark.nginx
def test_nginx_storage(storage_factory):
    with storage_factory() as local, storage_factory(root=STORAGE_ROOT, exist_ok=True) as remote:
        key = remote.write(__file__)
        with pytest.raises(ReadError):
            local.read(load_text, key)

        both = HashKeyStorage(local._local, Nginx(STORAGE_URL))
        assert both.read(load_text, key) == load_text(__file__)
        local.read(load_text, key)


@pytest.mark.nginx
def test_missing(storage_factory):
    with storage_factory(root=STORAGE_ROOT, exist_ok=True) as remote:
        location = Nginx(STORAGE_URL)
        location._get_config()

        key = remote.write(__file__)
        missing = key[::-1]
        for k, v in location.read_batch([key, missing]):
            if k == key:
                assert v is not None
                with v as result:
                    assert result.read() == Path(__file__).read_bytes()
            else:
                assert v is None


def test_wrong_address():
    assert list(Nginx('http://localhost/wrong').read_batch([b'some-key'])) == [(b'some-key', None)]
