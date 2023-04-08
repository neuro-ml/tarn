from ..location.nginx import Nginx


class HTTPLocation(Nginx):
    def __init__(self, url: str, optional: bool = False):
        super().__init__(url)
