import pytest

from tarn import Nginx, ReadError


def load_text(path):
    with open(path, 'r') as file:
        return file.read()


STORAGE_ROOT = '/tmp/http'
STORAGE_URL = 'http://localhost/'


@pytest.mark.nginx
def test_nginx_storage(storage_factory):
    with storage_factory() as local, storage_factory(root=STORAGE_ROOT, exist_ok=True) as remote:
        key = remote.write(__file__)
        with pytest.raises(ReadError):
            local.resolve(key)

        # add a remote
        local.storage.remote = [Nginx(STORAGE_URL)]
        with pytest.raises(ReadError, match=r'^Key \w+ is not present locally$'):
            local.read(load_text, key, fetch=False)

        assert local.read(load_text, key) == remote.read(load_text, key) == load_text(__file__)
        local.read(load_text, key)


@pytest.mark.nginx
def test_missing(storage_factory):
    with storage_factory(root=STORAGE_ROOT, exist_ok=True) as remote:
        location = Nginx(STORAGE_URL)
        with pytest.raises(AssertionError):
            location._get_config(None)

        key = remote.write(__file__)
        missing = key[:-1] + 'x'
        result = list(location.fetch([key, missing], lambda k, base: (base / 'data').exists(), location.hash))
        assert result == [(True, True), (None, False)]


def test_wrong_address():
    assert list(Nginx('http://localhost/wrong').read_batch([b'some-key'])) == [(b'some-key', None)]
