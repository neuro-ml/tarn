from paramiko.config import SSH_PORT

from ..location.scp import SCP


class SSHLocation(SCP):
    def __init__(self, hostname: str, root, port: int = SSH_PORT, username: str = None, password: str = None,
                 key=(), optional: bool = False):
        super().__init__(hostname, root, port, username, password, key)
