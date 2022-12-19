import pytest
import requests

from tarn import HTTPLocation, ReadError


def load_text(path):
    with open(path, 'r') as file:
        return file.read()


STORAGE_ROOT = '/tmp/http'
STORAGE_URL = 'http://localhost/'


@pytest.mark.nginx
def test_http_storage(storage_factory):
    with storage_factory() as local, storage_factory(root=STORAGE_ROOT, exist_ok=True) as remote:
        key = remote.write(__file__)
        with pytest.raises(ReadError):
            local.resolve(key)

        # add a remote
        local.storage.remote = [HTTPLocation(STORAGE_URL)]
        with pytest.raises(ReadError, match=r'^Key \w+ is not present locally$'):
            local.read(load_text, key, fetch=False)

        assert local.read(load_text, key) == remote.read(load_text, key) == load_text(__file__)
        local.read(load_text, key)


@pytest.mark.nginx
def test_missing(storage_factory):
    with storage_factory(root=STORAGE_ROOT, exist_ok=True) as remote:
        location = HTTPLocation(STORAGE_URL)
        with pytest.raises(AssertionError):
            location._get_config(None)

        key = remote.write(__file__)
        missing = key[:-1] + 'x'
        result = list(location.fetch([key, missing], lambda k, base: (base / 'data').exists(), location.hash))
        assert result == [(True, True), (None, False)]


def test_wrong_address():
    with pytest.raises(requests.exceptions.ConnectionError):
        list(HTTPLocation('http://localhost/wrong').fetch(['some-key'], lambda *args: True, None))

    assert list(HTTPLocation(
        'http://localhost/wrong', True
    ).fetch(['some-key'], lambda *args: True, None)) == [(None, False)]
