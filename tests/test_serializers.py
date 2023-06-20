import numpy as np
import pytest

from tarn.cache.serializers import JsonSerializer, NumpySerializer, PickleSerializer, DictSerializer
from tarn.digest import value_to_buffer


@pytest.mark.parametrize('serializer,values', (
    (JsonSerializer(), [1, 2, {'2': 3}, [1]]),
    (NumpySerializer(), [np.array(1, int), np.array(False, bool), np.array(1123.1123, float), np.array([5])]),
    (PickleSerializer(), [1, '2', 3.3, {1: 2}, np.array(2.5, float)]),
    (DictSerializer(JsonSerializer()), [{'a': 2, 'b': [1, 2, 3]}])
))
def test_serializers(serializer, values, disk_cache_factory):
    with disk_cache_factory(serializer) as cache:
        for value in values:
            cache.write(value, value)
            assert cache.read(value) == value


@pytest.mark.parametrize('serializer,first,second', (
    (JsonSerializer(), {'a': 1, 'b': 2}, {'b': 2, 'a': 1}),
    (DictSerializer(NumpySerializer()), {'a': np.array(1), 'b': np.array(2)}, {'b': np.array(2), 'a': np.array(1)}),
))
def test_serializers_determinism(serializer, first, second):
    assert list(map(to_bytes, serializer.save(first))) == list(map(to_bytes, serializer.save(second)))


def to_bytes(x):
    with value_to_buffer(x[1]) as buffer:
        return buffer.read()
