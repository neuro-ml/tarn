import filecmp
from pathlib import Path
from typing import Union, BinaryIO

from .compat import set_path_attrs
from .interface import Key  # noqa

PathLike = Union[Path, str]


def to_read_only(path: Path, permissions, group):
    adjust_permissions(path, permissions, group, read_only=True)


def adjust_permissions(path: Path, permissions, group, read_only: bool = False):
    if read_only:
        permissions = 0o444 & permissions

    set_path_attrs(path, permissions, group)


def get_size(file: Path) -> int:
    return file.stat().st_size


def mkdir(path: Path, permissions: Union[int, None], group: Union[str, int, None],
          parents: bool = False, exist_ok: bool = False):
    path.mkdir(parents=parents, exist_ok=exist_ok)
    set_path_attrs(path, permissions, group)


def create_folders(path: Path, permissions, group):
    if not path.exists():
        create_folders(path.parent, permissions, group)
        mkdir(path, permissions, group)


# TODO: need functions that return bool

def match_files(first: Path, second: Path):
    if not filecmp.cmp(first, second, shallow=False):
        raise ValueError(f'Files do not match: {first} vs {second}')


def match_buffers(first: BinaryIO, second: BinaryIO, context: str):
    bufsize = 8 * 1024
    while True:
        b1 = first.read(bufsize)
        b2 = second.read(bufsize)
        if b1 != b2:
            raise ValueError(f'Buffers do not match: {context}')
        if not b1:
            return True
