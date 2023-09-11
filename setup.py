import runpy
from pathlib import Path

from setuptools import find_packages, setup

classifiers = [
    'Development Status :: 4 - Beta',
    'License :: OSI Approved :: Apache Software License',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
    'Programming Language :: Python :: 3.8',
    'Programming Language :: Python :: 3.9',
    'Programming Language :: Python :: 3.10',
    'Programming Language :: Python :: 3.11',
    'Programming Language :: Python :: 3 :: Only',
]

name = 'tarn'
root = Path(__file__).resolve().parent
with open(root / 'requirements.txt', encoding='utf-8') as file:
    requirements = file.read().splitlines()
with open(root / 'README.md', encoding='utf-8') as file:
    long_description = file.read()
version = runpy.run_path(root / name / '__version__.py')['__version__']

setup(
    name=name,
    packages=find_packages(include=(name,)),
    include_package_data=True,
    version=version,
    author='Max',
    author_email='max@ira-labs.com',
    description='A generic framework for key-value storage',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/neuro-ml/tarn',
    download_url='https://github.com/neuro-ml/tarn/archive/v%s.tar.gz' % version,
    keywords=['storage', 'cache', 'invalidation'],
    classifiers=classifiers,
    install_requires=requirements,
    python_requires='>=3.7',
)
