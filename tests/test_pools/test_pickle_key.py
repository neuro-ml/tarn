from tarn import PickleKeyStorage
from tarn.serializers import Serializer


# TODO: reuse
def _mkdir(x):
    x.mkdir(parents=True, exist_ok=True)
    (x / 'config.yml').write_text('{levels: [1, 31], hash: sha256}')
    return x


def test_index_write(temp_dir):
    pool = PickleKeyStorage(_mkdir(temp_dir / 'index'), _mkdir(temp_dir / 'storage'), DifferentOrder())
    pool.write(0, 'ab')
    pool.write(0, 'ba')


class DifferentOrder(Serializer):
    def save(self, value, write):
        for v in value:
            yield v, write(v.encode())

    def load(self, contents, read):
        pass
