import filecmp
from pathlib import Path

import numpy as np
import pytest

from tarn.cache.serializers import JsonSerializer, NumpySerializer, PickleSerializer, DictSerializer


def test_serializers(disk_cache_factory):
    config = {
        JsonSerializer(): [1, 2, {'2': 3}, [1]],
        NumpySerializer(): [np.array(1, int), np.array(False, bool), np.array(1123.1123, float), np.array([5])],
        PickleSerializer(): [1, '2', 3.3, {1: 2}, np.array(2.5, float)],
        DictSerializer(JsonSerializer()): [{'a': 2, 'b': [1, 2, 3]}]
    }
    for s, values in config.items():
        with disk_cache_factory(s) as cache:
            for value in values:
                cache.write(value, value)
                assert cache.read(value) == value


@pytest.mark.parametrize('serializer,first,second', (
        (JsonSerializer(), {'a': 1, 'b': 2}, {'b': 2, 'a': 1}),
        (DictSerializer(NumpySerializer()), {'a': np.array(1), 'b': np.array(2)}, {'b': np.array(2), 'a': np.array(1)}),
))
def test_serializers_determinism(serializer, first, second, temp_dir):
    a, b = temp_dir / 'a', temp_dir / 'b'
    a.mkdir()
    b.mkdir()
    serializer.save(first, a)
    serializer.save(second, b)
    assert same_folders(filecmp.dircmp(a, b))


# source: https://stackoverflow.com/a/37790329
def same_folders(dcmp):
    if dcmp.diff_files or dcmp.left_only or dcmp.right_only:
        return False
    for sub_dcmp in dcmp.subdirs.values():
        if not same_folders(sub_dcmp):
            return False
    for name in dcmp.same_files:
        if not filecmp.cmp(Path(dcmp.left) / name, Path(dcmp.right) / name, shallow=False):
            return False
    return True
