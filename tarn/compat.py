import errno
import os
import platform
import shutil
import stat
from pathlib import Path
from tempfile import SpooledTemporaryFile as _SpooledTemporaryFile
from typing import Any, Union

from .interface import PathOrStr

# patches for various versions
try:
    # >=3.8
    from typing import Protocol
except ImportError:
    Protocol = object
try:
    # >=3.8
    from gzip import BadGzipFile
except ImportError:
    BadGzipFile = OSError
try:
    # >=3.11
    from typing import Self
except ImportError:
    Self = Any
try:
    # just a convenience lib for typing
    from mypy_boto3_s3 import S3Client
except ImportError:
    S3Client = Any
# we will try to support both versions 1 and 2 while they are more or less popular
try:
    from pydantic import field_validator as _field_validator, model_validator, BaseModel


    def field_validator(*args, always=None, **kwargs):
        # we just ignore `always`
        return _field_validator(*args, **kwargs)


    def model_validate(cls, data):
        return cls.model_validate(data)


    def model_dump(obj):
        return obj.model_dump()


    class NoExtra(BaseModel):
        model_config = {
            'extra': 'forbid'
        }


except ImportError:
    from pydantic import root_validator, validator as _field_validator, BaseModel


    def model_validator(mode: str):
        assert mode == 'before'
        return root_validator(pre=True)


    def field_validator(*args, mode: str = 'after', **kwargs):
        # we just ignore `always`
        assert mode in ('before', 'after')
        if mode == 'before':
            kwargs['pre'] = True
        return _field_validator(*args, **kwargs)


    def model_validate(cls, data):
        return cls.parse_obj(data)


    def model_dump(obj):
        return obj.dict()


    class NoExtra(BaseModel):
        class Config:
            extra = 'forbid'

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

if hasattr(_SpooledTemporaryFile, 'seekable'):
    SpooledTemporaryFile = _SpooledTemporaryFile
else:
    class SpooledTemporaryFile(_SpooledTemporaryFile):
        def seekable(self) -> bool:
            return True


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
