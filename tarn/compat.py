import errno
import os
import platform
import shutil
import stat
from contextlib import suppress
from pathlib import Path
from typing import Union

if platform.system() == 'Windows':
    def rmtree(path, ignore_errors=False):
        # source: https://docs.python.org/3.10/library/shutil.html#rmtree-example
        def remove_readonly(func, p, _):
            os.chmod(p, stat.S_IWRITE)
            func(p)

        shutil.rmtree(path, ignore_errors=ignore_errors, onerror=remove_readonly)

else:
    rmtree = shutil.rmtree


def set_path_attrs(path: Path, permissions: Union[int, None] = None, group: Union[str, int, None] = None):
    if permissions is not None:
        path.chmod(permissions)
    if group is not None:
        shutil.chown(path, group=group)


def get_path_group(path: Path):
    with suppress(NotImplementedError):
        # this will trigger an error on windows
        path.group()
        return path.stat().st_gid


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
