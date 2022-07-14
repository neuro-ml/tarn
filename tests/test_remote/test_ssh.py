import pytest

from tarn import SSHLocation, ReadError


def load_text(path):
    with open(path, 'r') as file:
        return file.read()


def get_ssh_location(root):
    return SSHLocation('remote', root, password='password')


@pytest.mark.ssh
def test_storage_ssh(storage_factory):
    with storage_factory() as local, storage_factory() as remote:
        key = remote.write(__file__)
        with pytest.raises(ReadError):
            local.resolve(key)

        # add a remote
        local.storage.remote = [get_ssh_location(remote.storage.levels[0].locations[0].root)]
        with pytest.raises(ReadError, match=r'^Key \w+ is not present locally$'):
            local.read(load_text, key, fetch=False)

        assert local.read(load_text, key) == remote.read(load_text, key) == load_text(__file__)
        local.read(load_text, key)
