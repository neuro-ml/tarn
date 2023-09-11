from scp import SCPClient, SCPException

from .remote import SSHRemote


class SCP(SSHRemote):
    exceptions = (SCPException, )

    def _client(self):
        return SCPClient(self.ssh.get_transport())
