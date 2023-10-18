from contextlib import contextmanager
from typing import ContextManager

from paramiko import SFTPClient

from .interface import SSHRemote


class SFTP(SSHRemote):
    exceptions = (FileNotFoundError, )

    @contextmanager
    def _client(self) -> ContextManager[SFTPClient]:
        sftp = self.ssh.open_sftp()
        yield sftp
        sftp.close()
