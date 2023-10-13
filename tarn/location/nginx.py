from contextlib import contextmanager
from typing import BinaryIO, ContextManager, Iterable, Tuple, Union
from urllib.parse import urljoin

import requests

from ..compat import Self
from ..config import load_config_buffer
from ..digest import key_to_relative
from ..interface import MaybeLabels, Meta
from .interface import Key, Keys, MaybeValue, ReadOnly


class Nginx(ReadOnly):
    def __init__(self, url: str):
        if not url.endswith('/'):
            url += '/'

        self.url = url
        self.levels = None

    # TODO: use a session

    def read_batch(self, keys: Keys) -> Iterable[Tuple[Key, Union[None, Tuple[BinaryIO, MaybeLabels]]]]:
        self._get_config()
        for key in keys:
            with self._read_single(key, True) as value:
                yield key, value

    def read(self, key: Key, return_labels: bool) -> ContextManager[Union[None, BinaryIO, Tuple[BinaryIO, None]]]:
        self._get_config()
        return self._read_single(key, return_labels)

    @contextmanager
    def _read_single(self, key: Key, return_labels) -> ContextManager[MaybeValue]:
        if self.levels is None:
            yield None
            return

        relative = key_to_relative(key, self.levels)
        with requests.get(urljoin(self.url, str(relative)), stream=True) as request:
            if not request.ok:
                yield None
                return

            with request.raw as raw:
                if return_labels:
                    yield raw, None
                else:
                    yield raw

    def contents(self) -> Iterable[Tuple[Key, Self, Meta]]:
        # TODO
        return []

    def _get_config(self):
        try:
            if self.levels is None:
                with requests.get(urljoin(self.url, 'config.yml'), stream=True) as request:
                    if not request.ok:
                        return
                    config = load_config_buffer(request.text)
                    self.levels = config.levels

        except requests.exceptions.RequestException:
            pass
