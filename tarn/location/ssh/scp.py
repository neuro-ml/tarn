from contextlib import contextmanager
from typing import ContextManager

from scp import SCPClient, SCPException

from .interface import SSHRemote


class SCP(SSHRemote):
    exceptions = (SCPException, )

    @contextmanager
    def _client(self) -> ContextManager[SCPClient]:
        scp = SCPClient(self.ssh.get_transport())
        yield scp
        scp.close()
