import errno
import os
import platform
import shutil
import stat
from pathlib import Path
from typing import Union

try:
    from typing import Protocol
except ImportError:
    Protocol = object

from .interface import PathOrStr

if platform.system() == 'Windows':
    def rmtree(path, ignore_errors=False):
        # source: https://docs.python.org/3.10/library/shutil.html#rmtree-example
        def remove_readonly(func, p, _):
            os.chmod(p, stat.S_IWRITE)
            func(p)

        shutil.rmtree(path, ignore_errors=ignore_errors, onerror=remove_readonly)


    def get_path_group(path: PathOrStr) -> Union[int, None]:
        pass


    def remove_file(path: PathOrStr):
        os.chmod(path, stat.S_IWRITE)
        os.remove(path)

else:
    rmtree = shutil.rmtree
    remove_file = os.remove


    def get_path_group(path: PathOrStr) -> Union[int, None]:
        return Path(path).stat().st_gid


def set_path_attrs(path: Path, permissions: Union[int, None] = None, group: Union[str, int, None] = None):
    if permissions is not None:
        path.chmod(permissions)
    if group is not None:
        shutil.chown(path, group=group)


def copy_file(source: PathOrStr, destination: PathOrStr):
    # in Python>=3.8 the sendfile call is used, which apparently may fail
    try:
        shutil.copyfile(source, destination)
    except OSError as e:
        # BlockingIOError -> fallback to slow copy
        if e.errno != errno.EWOULDBLOCK:
            raise

        with open(source, 'rb') as src, open(destination, 'wb') as dst:
            shutil.copyfileobj(src, dst)


class HashAlgorithm(Protocol):
    digest_size: int

    def update(self, chunk: bytes) -> None:
        pass

    def digest(self) -> bytes:
        pass
