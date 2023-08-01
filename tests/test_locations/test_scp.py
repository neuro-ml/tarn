from pathlib import Path

import pytest

from tarn import SCP, HashKeyStorage, ReadError


def load_text(path):
    with open(path, 'r') as file:
        return file.read()


def get_ssh_location(root):
    return SCP('remote', root, password='password')


STORAGE_ROOT = '/tmp/scp'


@pytest.mark.ssh
def test_storage_ssh(storage_factory):
    with storage_factory() as local, storage_factory(root=STORAGE_ROOT, exist_ok=True, names=None) as remote:
        key = remote.write(Path(__file__))
        with pytest.raises(ReadError):
            local.read(load_text, key)

        # add a remote
        both = HashKeyStorage(local._local, get_ssh_location(STORAGE_ROOT))
        assert both.read(load_text, key) == load_text(__file__)
        local.read(load_text, key)


def test_wrong_host():
    assert list(SCP('localhost', '/').read_batch([b'some-key'])) == [(b'some-key', None)]
