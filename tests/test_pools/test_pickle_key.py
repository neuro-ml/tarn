from tarn import PickleKeyStorage
from tarn.serializers import JsonSerializer, DictSerializer


# TODO: reuse
def _mkdir(x):
    x.mkdir(parents=True, exist_ok=True)
    (x / 'config.yml').write_text('{levels: [1, 31], hash: sha256}')
    return x


def test_index_write(temp_dir):
    pool = PickleKeyStorage(_mkdir(temp_dir / 'index'), _mkdir(temp_dir / 'storage'), DictSerializer(JsonSerializer()))
    pool.write(temp_dir, {'a': [1, 2, 3], 'b': [4, 5, 6]})
    pool.write(temp_dir, {'b': [4, 5, 6], 'a': [1, 2, 3]})
