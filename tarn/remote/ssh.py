import contextlib
import socket
import tempfile
from pathlib import Path
from typing import Union, Sequence, Callable, Any, Tuple, Iterable

import paramiko
from paramiko import SSHClient, AuthenticationException, SSHException
from paramiko.config import SSH_PORT, SSHConfig
from paramiko.ssh_exception import NoValidConnectionsError
from scp import SCPClient, SCPException

from ..compat import rmtree
from ..config import load_config, HashConfig
from ..digest import key_to_relative
from ..interface import RemoteStorage, Key
from ..utils import PathLike


class UnknownHostException(SSHException):
    pass


class SSHLocation(RemoteStorage):
    def __init__(self, hostname: str, root: PathLike, port: int = SSH_PORT, username: str = None, password: str = None,
                 key: Union[Path, Sequence[Path]] = (), optional: bool = False):
        ssh = SSHClient()
        ssh.load_system_host_keys()
        # TODO: not safe
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        if isinstance(key, Path):
            key = str(key)

        config_path = Path('~/.ssh/config').expanduser()
        if config_path.exists():
            with open(config_path) as f:
                config = SSHConfig()
                config.parse(f)
                host = config.lookup(hostname)

                hostname = host.get('hostname', hostname)
                port = host.get('port', port)
                username = host.get('user', username)
                key = host.get('identityfile', key)

        self.hostname, self.port, self.username, self.password, self.key = hostname, port, username, password, key
        self.root = Path(root)
        self.ssh = ssh
        self.levels = self.hash = None
        self.optional = optional

    def fetch(self, keys: Sequence[Key], store: Callable[[Key, Path], Any],
              config: HashConfig) -> Iterable[Tuple[Any, bool]]:

        with self._connect(config) as scp:
            if scp is None:
                yield from [(None, False)] * len(keys)
                return

            with tempfile.TemporaryDirectory() as temp_dir:
                source = Path(temp_dir) / 'source'
                for key in keys:
                    try:
                        scp.get(str(self.root / key_to_relative(key, self.levels)), str(source), recursive=True)
                        if not source.exists():
                            yield None, False

                        else:
                            value = store(key, source)
                            rmtree(source)
                            yield value, True

                    except (SCPException, socket.timeout, SSHException):
                        yield None, False

                    rmtree(source, ignore_errors=True)

    def push(self, keys: Sequence[Key], resolve: Callable[[Key], Path], config: HashConfig) -> Iterable[bool]:
        with self._connect(config) as scp:
            if scp is None:
                yield from [False] * len(keys)
                return

            for key in keys:
                try:
                    scp.put(resolve(key), str(self.root / key_to_relative(key, self.levels)), recursive=True)
                    yield True

                except (SCPException, socket.timeout, SSHException):
                    yield False

    @contextlib.contextmanager
    def _connect(self, config):
        try:
            self.ssh.connect(
                self.hostname, self.port, self.username, self.password, key_filename=self.key,
                auth_timeout=10
            )
        except AuthenticationException:
            raise AuthenticationException(self.hostname) from None
        except socket.gaierror:
            raise UnknownHostException(self.hostname) from None
        except (SSHException, NoValidConnectionsError):
            if not self.optional:
                raise

            yield None
            return

        try:
            with SCPClient(self.ssh.get_transport()) as scp:
                if not self._get_config(scp, config):
                    yield None
                else:
                    yield scp

        finally:
            self.ssh.close()

    def _get_config(self, scp, config):
        try:
            if self.levels is None:
                with tempfile.TemporaryDirectory() as temp_dir:
                    temp = Path(temp_dir) / 'config.yml'
                    scp.get(str(self.root / 'config.yml'), str(temp))
                    remote_config = load_config(temp_dir)
                    self.hash, self.levels = remote_config.hash, remote_config.levels

            assert self.hash == config, (self.hash, config)
            return True

        except SCPException:
            if not self.optional:
                raise

            return False
