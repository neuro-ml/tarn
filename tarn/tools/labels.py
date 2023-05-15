import json
from abc import ABC, abstractmethod
from pathlib import Path

from ..compat import remove_file, get_path_group, set_path_attrs
from ..digest import key_to_relative
from ..interface import MaybeLabels, Key

__all__ = 'LabelsStorage', 'DummyLabels'

from ..utils import create_folders


class LabelsStorage(ABC):
    def __init__(self, root: Path):
        self.root = root

    @abstractmethod
    def set(self, key: Key, labels: MaybeLabels):
        pass

    @abstractmethod
    def get(self, key: Key) -> MaybeLabels:
        pass


class DummyLabels(LabelsStorage):
    def set(self, key: Key, labels: MaybeLabels):
        pass

    def get(self, key: Key) -> MaybeLabels:
        pass


class JsonLabels(LabelsStorage):
    def set(self, key: Key, labels: MaybeLabels):
        file = self._file(key)
        missing = not file.exists()

        if labels is None:
            if not missing:
                remove_file(file)
            return

        group = get_path_group(self.root)
        create_folders(file.parent, 0o777, group)
        file.write_text(json.dumps(list(labels)))

        if missing:
            set_path_attrs(file, 0o777, group)

    def get(self, key: Key) -> MaybeLabels:
        file = self._file(key)
        if not file.exists():
            return

        return json.loads(file.read_text())

    def _file(self, key):
        return (self.root / key_to_relative(key, (1, len(key) - 1))).with_suffix('.json')
