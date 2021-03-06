import sys

import pytest

from pickler_test_helpers import functions, classes

# using sha256 here is enough because we can always access the commit used to generate it
REFERENCES = {
    (3, 9): {
        (1, functions.identity): '9b8b1e3ae950963e609f8164ba76403c6790731db5e1cbbdb56a0cf9cd886005',
        (1, functions.nested_identity): '9e23d47084c40f37054da37a345e26fd85bbf5ad733d84f790acb03d8e16d115',
        (1, classes.One): '4acfbb8cc1dfb43db698ebedc9c6eb0b23d71b2dc021e2f684cfa321b8d5aa01',
        (1, classes.A): '47de846b6392330138bde757e463d6f420496a007ec7407c5933678b3a0f0c89',
    }
}


@pytest.fixture
def pickle_references():
    return REFERENCES.get(sys.version_info[:2], {})
