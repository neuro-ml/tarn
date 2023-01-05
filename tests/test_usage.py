import platform

from tarn.tools.usage import StatUsage


def test_permissions(temp_dir):
    usage = StatUsage()
    usage.update('', temp_dir)
    mark = temp_dir / '.time'
    assert mark.exists()
    if platform.system() != 'Windows':
        assert mark.stat().st_mode & 0o777 == 0o777
