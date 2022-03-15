import hashlib
from functools import partial
from pathlib import Path
from typing import Union, Dict, Any, Tuple, Sequence

import humanfriendly
from pydantic import BaseModel, Extra, validator, root_validator
from yaml import safe_load, safe_dump

from .tools import Locker, DummyLocker, SizeTracker, DummySize, UsageTracker, DummyUsage
from .utils import PathLike, mkdir

CONFIG_NAME = 'config.yml'


class _NoExtra(BaseModel):
    class Config:
        extra = Extra.forbid


class HashConfig(_NoExtra):
    name: str
    kwargs: Dict[str, Any] = None

    @root_validator(pre=True)
    def normalize_kwargs(cls, values):
        alias = 'kwargs'
        required = {field.alias for field in cls.__fields__.values() if field.alias != alias}

        kwargs = {}
        for field_name in list(values):
            if field_name not in required:
                kwargs[field_name] = values.pop(field_name)

        values[alias] = kwargs
        return values

    def dict(self, **kwargs):
        real = super().dict(**kwargs)
        kwargs = real.pop('kwargs')
        real.update(kwargs)
        return real

    def build(self):
        cls = getattr(hashlib, self.name)
        if self.kwargs:
            cls = partial(cls, **self.kwargs)
        return cls


class ToolConfig(_NoExtra):
    name: str
    args: Tuple = ()
    kwargs: Dict[str, Any] = None

    @validator('kwargs', always=True)
    def normalize_kwargs(cls, v):
        if v is None:
            return {}
        return v


class StorageConfig(_NoExtra):
    hash: HashConfig
    levels: Sequence[int]
    locker: ToolConfig = None
    size: ToolConfig = None
    usage: ToolConfig = None
    free_disk_size: Union[int, str] = 0
    max_size: Union[int, str] = None
    version: str = None

    @staticmethod
    def _make(base, dummy, config):
        if config is None:
            return dummy()
        cls = find_subclass(base, config.name)
        return cls(*config.args, **config.kwargs)

    def make_locker(self) -> Locker:
        return self._make(Locker, DummyLocker, self.locker)

    def make_size(self) -> SizeTracker:
        return self._make(SizeTracker, DummySize, self.size)

    def make_usage(self) -> UsageTracker:
        return self._make(UsageTracker, DummyUsage, self.usage)

    @validator('free_disk_size', 'max_size')
    def to_size(cls, v):
        return parse_size(v)

    @validator('hash', 'locker', 'usage', pre=True)
    def normalize_tools(cls, v):
        if isinstance(v, str):
            v = {'name': v}
        return v


def root_params(root: Path):
    stat = root.stat()
    return stat.st_mode & 0o777, stat.st_gid


def load_config(root: PathLike) -> StorageConfig:
    with open(Path(root) / CONFIG_NAME) as file:
        return StorageConfig.parse_obj(safe_load(file))


def parse_size(x):
    if isinstance(x, int):
        return x
    if isinstance(x, str):
        return humanfriendly.parse_size(x)
    if x is not None:
        raise ValueError(f"Couldn't understand the size format: {x}")


def find_subclass(base, name):
    for cls in base.__subclasses__():
        if cls.__name__ == name:
            return cls

    raise ValueError(f'Could not find a {base.__name__} named {name}')


def init_storage(config: StorageConfig, root: PathLike, *,
                 permissions: Union[int, None] = None, group: Union[str, int, None] = None, exist_ok: bool = False):
    root = Path(root)
    mkdir(root, permissions, group, parents=True, exist_ok=exist_ok)

    with open(root / CONFIG_NAME, 'w') as file:
        safe_dump(config.dict(), file)
