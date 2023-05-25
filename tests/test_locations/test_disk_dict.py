import filecmp
import io
import platform
import time
from multiprocessing import Process
from pathlib import Path

import pytest

from tarn import HashKeyStorage, DiskDict, Levels, Fanout
from tarn.config import init_storage, StorageConfig


def test_storage_fixture(storage_factory):
    with storage_factory() as storage:
        pass
    assert not any(x.root.exists() for x in storage._local._locations)

    with storage_factory(names=['1', '2', '3']) as storage:
        pass
    assert not any(x.root.exists() for x in storage._local._locations)


def test_read_only(random_disk_dict):
    location = random_disk_dict
    key, value = b'0' * sum(location.levels), Path(__file__)
    # neither during write
    with location.write(key, value, None) as result:
        assert result.read_bytes() == value.read_bytes()

        with pytest.raises(PermissionError):
            result.write_bytes(b'')

    # nor read
    with location.read(key) as result:
        assert result.read_bytes() == value.read_bytes()

        with pytest.raises(PermissionError):
            result.write_bytes(b'')

    # nor repeated write
    with location.write(key, value, None) as result:
        assert result.read_bytes() == value.read_bytes()

        with pytest.raises(PermissionError):
            result.write_bytes(b'')


def test_single_local(storage_factory):
    with storage_factory({'name': 'GlobalThreadLocker'}) as storage:
        disk = storage._local._locations[0]

        # just store this file, because why not
        file = Path(__file__)
        permissions = file.stat().st_mode & 0o777
        key = storage.write(file)
        stored = storage.read(lambda x: x, key)

        assert filecmp.cmp(file, stored, shallow=False)
        assert file.stat().st_mode & 0o777 == permissions
        assert stored.stat().st_mode & 0o777 == disk.permissions & 0o444
        if platform.system() != 'Windows':
            assert stored.stat().st_gid == disk.group
        else:
            assert disk.group is None


def test_layers(temp_dir):
    target = Path(__file__)
    for name in ['a1', 'b1', 'b2', 'c1']:
        init_storage(StorageConfig(hash='blake2b', levels=[1, 63]), temp_dir / name)

    a1 = DiskDict(temp_dir / 'a1')
    b1 = DiskDict(temp_dir / 'b1')
    b2 = DiskDict(temp_dir / 'b2')
    c1 = DiskDict(temp_dir / 'c1')

    storage = HashKeyStorage(Levels(
        a1, Fanout(b1, b2), c1,
    ))

    key = storage.write(target)
    assert key == storage.write(target)

    # only the first level is written to
    assert len(_glob(a1)) == 1
    for x in [b1, b2, c1]:
        assert len(_glob(x)) == 0

    # now spin the replication
    storage = HashKeyStorage(Levels(
        Fanout(b1, b2), c1, a1,
    ))
    # replicate
    storage.read(lambda x: x, key)

    # now the first level is written to
    for x in [a1, b1]:
        assert len(_glob(x)) == 1, x.root

    for x in [b2, c1]:
        assert len(_glob(x)) == 0, x.root


# FIXME: this test depends on the speed of the storage
def test_process_kill(random_disk_dict):
    key = b'\x00' * sum(random_disk_dict.levels)
    total_size = 100 * 1024 ** 2
    root = random_disk_dict.root

    # we start a process and kill it abruptly
    p = Process(target=_write, args=(root, key, total_size))
    p.start()
    time.sleep(0.1)
    # TODO: remove after py3.6 is dropped
    p.kill() if hasattr(p, 'kill') else p.terminate()
    p.close()

    with random_disk_dict.read(key) as result:
        assert result is None, (result.stat().st_size, total_size)


def _glob(location):
    return list(set(location.root.glob('*/*')) - set((location.root / 'tools').glob('*')))


def _write(root, key, total_size):
    d = DiskDict(root)
    # 100 megabytes of garbage
    with d.write(key, io.BytesIO(bytearray(total_size)), None):
        pass
