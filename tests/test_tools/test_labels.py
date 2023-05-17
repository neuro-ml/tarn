import pytest
from tarn.tools.labels import JsonLabels


def test_interface(temp_dir):
    labels = JsonLabels(temp_dir)
    key = b'\x00' * 32
    assert labels.get(key) is None

    labels.update(key, ['a', 'b', 'c'])
    assert set(labels.get(key)) == set('abc')

    labels.update(key, None)
    assert set(labels.get(key)) == set('abc')
    labels.update(key, [])
    assert set(labels.get(key)) == set('abc')

    labels.update(key, ['d', 'e'])
    assert set(labels.get(key)) == set('abcde')

    labels.delete(key)
    assert labels.get(key) is None


def test_wrong_type(temp_dir):
    labels = JsonLabels(temp_dir)
    with pytest.raises(TypeError):
        labels.update(b'\x00' * 32, 'abc')
