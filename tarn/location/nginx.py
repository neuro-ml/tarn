from contextlib import contextmanager
from typing import BinaryIO, ContextManager, Iterable, Optional
from urllib.parse import urljoin

import requests

from ..config import load_config_buffer
from ..digest import key_to_relative
from .disk_dict import Key
from .interface import Keys, Location, MaybeValue


class Nginx(Location):
    def __init__(self, url: str):
        if not url.endswith('/'):
            url += '/'

        self.url = url
        self.levels = self.hash = None

    @property
    def key_size(self):
        return sum(self.levels) if self.levels is not None else None

    # TODO: use a session

    def read_batch(self, keys: Keys) -> Iterable[ContextManager[MaybeValue]]:
        self._get_config()
        for key in keys:
            with self._read_single(key) as value:
                yield key, value

    def read(self, key: Key) -> ContextManager[Optional[BinaryIO]]:
        self._get_config()
        return self._read_single(key)

    @contextmanager
    def _read_single(self, key: Key) -> ContextManager[MaybeValue]:
        if self.levels is None:
            yield None
            return

        relative = key_to_relative(key, self.levels)
        with requests.get(urljoin(self.url, str(relative)), stream=True) as request:
            if not request.ok:
                # TODO: this is probably an old format directory
                with requests.get(urljoin(self.url, str(relative / 'data')), stream=True) as req:
                    if req.ok:
                        with req.raw as raw:
                            yield raw
                            return

                yield None
                return

            with request.raw as raw:
                yield raw

    def _get_config(self):
        try:
            if self.levels is None:
                with requests.get(urljoin(self.url, 'config.yml'), stream=True) as request:
                    if not request.ok:
                        return
                    config = load_config_buffer(request.text)
                    self.hash, self.levels = config.hash.build(), config.levels

        except requests.exceptions.RequestException:
            pass
