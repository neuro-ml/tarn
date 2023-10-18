from pathlib import Path

import pytest

from tarn import SFTP, HashKeyStorage, ReadError


def load_text(path):
    with open(path, 'r') as file:
        return file.read()


def get_ssh_location(root):
    return SFTP('remote', root, password='password')


STORAGE_ROOT = '/tmp/sfpt'


@pytest.mark.ssh
def test_storage_ssh(storage_factory):
    with storage_factory() as local, storage_factory(root=STORAGE_ROOT, exist_ok=True, names=None) as remote:
        key = remote.write(Path(__file__))
        with pytest.raises(ReadError):
            local.read(load_text, key)

        # add a remote
        ssh_location = get_ssh_location(STORAGE_ROOT)
        both = HashKeyStorage(local._local, ssh_location)
        assert both.read(load_text, key) == load_text(__file__)
        local.read(load_text, key)
        with ssh_location.read(b'123213213332', False) as v:
            assert v is None


def test_wrong_host():
    assert list(SFTP('localhost', '/').read_batch([b'some-key'])) == [(b'some-key', None)]
