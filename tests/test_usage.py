from pond.tools.usage import StatUsage


def test_permissions(temp_dir):
    usage = StatUsage()
    usage.update('', temp_dir)
    assert (temp_dir / '.time').stat().st_mode & 0o777 == 0o777
