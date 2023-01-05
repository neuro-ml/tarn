import tempfile
from pathlib import Path
from typing import Sequence, Callable, Any, Tuple, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import urlretrieve

import requests

from ..compat import rmtree
from ..config import load_config, HashConfig
from ..digest import key_to_relative
from ..interface import RemoteStorage, Key


class HTTPLocation(RemoteStorage):
    def __init__(self, url: str, optional: bool = False):
        if not url.endswith('/'):
            url += '/'

        self.url = url
        self.optional = optional
        self.levels = self.hash = None

    def fetch(self, keys: Sequence[Key], store: Callable[[Key, Path], Any],
              config: HashConfig) -> Iterable[Tuple[Any, bool]]:

        with tempfile.TemporaryDirectory() as temp_dir:
            source = Path(temp_dir) / 'source'
            if keys and not self._get_config(config):
                yield from [(None, False)] * len(keys)
                return

            for key in keys:
                try:
                    self._fetch_tree(key_to_relative(key, self.levels), source)

                    value = store(key, source)
                    rmtree(source)
                    yield value, True

                except requests.exceptions.ConnectionError:
                    yield None, False

                rmtree(source, ignore_errors=True)

    def _fetch_one(self, relative, local):
        try:
            urlretrieve(urljoin(self.url, str(relative)), str(local))
        except (HTTPError, URLError) as e:
            raise requests.exceptions.ConnectionError from e

    def _fetch_tree(self, relative, local):
        local.mkdir(parents=True)

        # we know that this is a directory listing in json format
        url = urljoin(self.url, str(relative))
        response = requests.get(url)
        if response.status_code != 200:
            raise requests.exceptions.ConnectionError(url)

        for entry in response.json():
            kind, name = entry['type'], entry['name']
            if kind == 'directory':
                self._fetch_tree(relative / name, local / name)
            else:
                assert kind == 'file', kind
                self._fetch_one(relative / name, local / name)

    def _get_config(self, config):
        try:
            if self.levels is None:
                with tempfile.TemporaryDirectory() as temp_dir:
                    temp = Path(temp_dir) / 'config.yml'
                    self._fetch_one('config.yml', temp)
                    remote_config = load_config(temp_dir)
                    self.hash, self.levels = remote_config.hash, remote_config.levels

            assert self.hash == config, (self.hash, config)
            return True

        except requests.exceptions.ConnectionError:
            if not self.optional:
                raise

            return False
