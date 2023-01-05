import filecmp
from pathlib import Path
from typing import Union

from .compat import set_path_attrs, copy_file  # noqa

PathLike = Union[Path, str]
Key = str


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


def match_files(first: Path, second: Path):
    if not filecmp.cmp(first, second, shallow=False):
        raise ValueError(f'Files do not match: {first} vs {second}')
