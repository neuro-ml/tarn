import hashlib
import io
from functools import partial
from pathlib import Path
from typing import Any, Dict, Optional, Sequence, Tuple, Union

import humanfriendly
from pydantic import Field
from yaml import safe_dump, safe_load

from .compat import field_validator, get_path_group, model_validator, model_validate, model_dump, NoExtra
from .interface import PathOrStr
from .tools import DummyLabels, DummyLocker, DummySize, DummyUsage, LabelsStorage, Locker, SizeTracker, UsageTracker
from .utils import mkdir

CONFIG_NAME = 'config.yml'


class HashConfig(NoExtra):
    name: str
    kwargs: Dict[str, Any] = None

    @model_validator(mode='before')
    def normalize_kwargs(cls, values):
        kwargs = {}
        for field_name in list(values):
            if field_name not in ('name', 'kwargs'):
                kwargs[field_name] = values.pop(field_name)

        values['kwargs'] = kwargs
        return values

    def model_dump(self, **kwargs):
        real = super().model_dump(**kwargs)
        kwargs = real.pop('kwargs')
        real.update(kwargs)
        return real

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


class ToolConfig(NoExtra):
    name: str
    args: Tuple = ()
    kwargs: Optional[Dict[str, Any]] = None

    @field_validator('kwargs', always=True)
    def normalize_kwargs(cls, v):
        if v is None:
            return {}
        return v


class StorageConfig(NoExtra):
    hash: HashConfig
    levels: Optional[Sequence[int]] = Field(None, validate_default=True)
    locker: Optional[ToolConfig] = None
    size: Optional[ToolConfig] = None
    usage: Optional[ToolConfig] = None
    labels: Optional[ToolConfig] = None
    free_disk_size: Union[int, str] = 0
    max_size: Optional[Union[int, str]] = None

    @staticmethod
    def _make(base, dummy, config, *args):
        if config is None:
            return dummy(*args)
        cls = find_subclass(base, config.name)
        return cls(*args, *config.args, **config.kwargs)

    def make_locker(self) -> Locker:
        return self._make(Locker, DummyLocker, self.locker)

    def make_size(self) -> SizeTracker:
        return self._make(SizeTracker, DummySize, self.size)

    def make_usage(self, root: Path) -> UsageTracker:
        return self._make(UsageTracker, DummyUsage, self.usage, root)

    def make_labels(self, root: Path) -> LabelsStorage:
        return self._make(LabelsStorage, DummyLabels, self.labels, root)

    @field_validator('free_disk_size', 'max_size')
    def to_size(cls, v):
        return parse_size(v)

    @field_validator('hash', 'locker', 'usage', 'labels', mode='before')
    def normalize_tools(cls, v):
        if isinstance(v, str):
            v = {'name': v}
        return v

    @field_validator('levels', always=True)
    def normalize_levels(cls, v, values):
        # default levels are [1, n - 1]
        if not isinstance(values, dict):
            values = values.data
        if v is None:
            v = 1, values['hash'].build()().digest_size - 1
        return v


def root_params(root: Path):
    stat = root.stat()
    return stat.st_mode & 0o777, get_path_group(root)


def load_config_buffer(data: str) -> StorageConfig:
    return model_validate(StorageConfig, safe_load(io.StringIO(data)))


def load_config(root: PathOrStr) -> StorageConfig:
    with open(Path(root) / CONFIG_NAME) as file:
        return model_validate(StorageConfig, safe_load(file))


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


def init_storage(config: StorageConfig, root: PathOrStr, *,
                 permissions: Union[int, None] = None, group: Union[str, int, None] = None, exist_ok: bool = False):
    root = Path(root)
    mkdir(root, permissions, group, parents=True, exist_ok=exist_ok)

    with open(root / CONFIG_NAME, 'w') as file:
        safe_dump(model_dump(config), file)
