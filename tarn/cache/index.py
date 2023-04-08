from ..interface import PathOrStr
from ..location import DiskDict


class CacheIndex(DiskDict):
    def __init__(self, root: PathOrStr, storage, serializer):
        super().__init__(root)
        self.storage, self.serializer = storage, serializer
