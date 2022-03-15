import filecmp
from pathlib import Path

from tarn import Storage, StorageLevel, Disk
from tarn.config import init_storage, StorageConfig


def test_storage_fixture(storage_factory):
    with storage_factory() as storage:
        pass
    assert not any(x.root.exists() for level in storage.levels for x in level.locations)

    with storage_factory(names=['1', '2', '3']) as storage:
        pass
    assert not any(x.root.exists() for level in storage.levels for x in level.locations)


def test_single_local(storage_factory):
    with storage_factory({'name': 'GlobalThreadLocker'}) as storage:
        disk = storage.levels[0].locations[0]

        # just store this file, because why not
        file = Path(__file__)
        permissions = file.stat().st_mode & 0o777
        key = storage.write(file)
        stored = storage.resolve(key)

        assert filecmp.cmp(file, stored, shallow=False)
        assert file.stat().st_mode & 0o777 == permissions
        assert stored.stat().st_mode & 0o777 == disk.permissions & 0o444
        assert stored.stat().st_gid == disk.group


def test_layers(temp_dir):
    target = Path(__file__)
    for name in ['a1', 'b1', 'b2', 'c1']:
        init_storage(StorageConfig(hash='blake2b', levels=[1, 63]), temp_dir / name)

    a1 = Disk(temp_dir / 'a1')
    b1 = Disk(temp_dir / 'b1')
    b2 = Disk(temp_dir / 'b2')
    c1 = Disk(temp_dir / 'c1')

    storage = Storage(
        StorageLevel([a1], True, True),
        StorageLevel([b1, b2], True, True),
        StorageLevel([c1], True, True),
    )

    key = storage.write(target)
    assert key == storage.write(target)
    storage.resolve(key)

    # only the first level is written to
    assert len(list(a1.root.iterdir())) == 2
    for x in [b1, b2, c1]:
        assert len(list(x.root.iterdir())) == 1

    # now spin the replication
    storage = Storage(
        StorageLevel([b1, b2], True, True),
        StorageLevel([c1], True, True),
        StorageLevel([a1], True, True),
    )
    # replicate
    storage.resolve(key)
    # check consistency
    storage.resolve(key)

    # now all levels are written to
    for x in [a1, b1, c1]:
        assert len(list(x.root.iterdir())) == 2, x.root
    # b1 and b2 are from the same level
    assert len(list(b2.root.iterdir())) == 1
