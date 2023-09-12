import datetime
import os
from pathlib import Path
from typing import BinaryIO, Collection, Optional, Sequence, Union

Key = bytes
Keys = Sequence[Key]
PathOrStr = Union[Path, str, os.PathLike]
Value = Union[BinaryIO, os.PathLike]
MaybeValue = Optional[Value]
MaybeLabels = Optional[Collection[str]]


class Meta:
    last_used: Optional[datetime.datetime]
    labels: MaybeLabels


# TODO: deprecated
class LocalStorage:
    pass


class RemoteStorage:
    pass
