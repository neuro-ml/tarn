import errno
import filecmp
import os
import shutil
from enum import Enum
from pathlib import Path
from typing import Union

PathLike = Union[Path, str]
Key = str


class Reason(Enum):
    WrongDigestSize, WrongHash, CorruptedHash, WrongFolderStructure, CorruptedData, Expired, Filtered = range(7)


def to_read_only(path: Path, permissions, group):
    os.chmod(path, 0o444 & permissions)
    shutil.chown(path, group=group)


def get_size(file: Path) -> int:
    return file.stat().st_size


def mkdir(path: Path, permissions: Union[int, None], group: Union[str, int, None],
          parents: bool = False, exist_ok: bool = False):
    path.mkdir(parents=parents, exist_ok=exist_ok)
    if permissions is not None:
        path.chmod(permissions)
    if group is not None:
        shutil.chown(path, group=group)


def create_folders(path: Path, permissions, group):
    if not path.exists():
        create_folders(path.parent, permissions, group)
        mkdir(path, permissions, group)


def copy_file(source, destination):
    # in Python>=3.8 the sendfile call is used, which apparently may fail
    try:
        shutil.copyfile(source, destination)
    except OSError as e:
        # BlockingIOError -> fallback to slow copy
        if e.errno != errno.EWOULDBLOCK:
            raise

        with open(source, 'rb') as src, open(destination, 'wb') as dst:
            shutil.copyfileobj(src, dst)


def match_files(first: Path, second: Path):
    if not filecmp.cmp(first, second, shallow=False):
        raise ValueError(f'Files do not match: {first} vs {second}')
