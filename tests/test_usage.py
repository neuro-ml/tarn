import platform

from tarn.tools.usage import StatUsage


def test_permissions(temp_dir):
    usage = StatUsage(temp_dir)
    usage.update(b'\x00' * 32, __file__)
    mark = temp_dir / '00' / ('0' * 62)
    assert mark.exists()
    if platform.system() != 'Windows':
        assert mark.stat().st_mode & 0o777 == 0o777
