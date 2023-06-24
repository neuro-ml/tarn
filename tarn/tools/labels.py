import json
from abc import ABC, abstractmethod
from pathlib import Path

from ..compat import get_path_group, remove_file, set_path_attrs
from ..digest import key_to_relative
from ..interface import Key, MaybeLabels

__all__ = 'LabelsStorage', 'DummyLabels'

from ..utils import create_folders


class LabelsStorage(ABC):
    def __init__(self, root: Path):
        self.root = root

    @abstractmethod
    def update(self, key: Key, labels: MaybeLabels):
        pass

    @abstractmethod
    def get(self, key: Key) -> MaybeLabels:
        pass

    @abstractmethod
    def delete(self, key: Key):
        pass


class DummyLabels(LabelsStorage):
    def update(self, key: Key, labels: MaybeLabels):
        pass

    def get(self, key: Key) -> MaybeLabels:
        pass
    
    def delete(self, key: Key):
        pass


class JsonLabels(LabelsStorage):
    def update(self, key: Key, labels: MaybeLabels):
        if labels is None:
            return
        
        if isinstance(labels, str):
            raise TypeError('Expected a collection of strings, not a string')

        file = self._file(key)
        missing = not file.exists()
        group = get_path_group(self.root)
        create_folders(file.parent, 0o777, group)

        if not missing:
            labels = set(labels) | set(json.loads(file.read_text()))
        file.write_text(json.dumps(list(labels)))

        if missing:
            set_path_attrs(file, 0o777, group)

    def get(self, key: Key) -> MaybeLabels:
        file = self._file(key)
        if not file.exists():
            return

        return json.loads(file.read_text())

    def delete(self, key: Key):
        file = self._file(key)
        if file.exists():
            remove_file(file)

    def _file(self, key):
        return (self.root / key_to_relative(key, (1, len(key) - 1))).with_suffix('.json')
