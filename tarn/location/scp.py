import socket
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import ContextManager, Iterable, Sequence, Tuple, Union

import paramiko
from paramiko import AuthenticationException, SSHClient, SSHException
from paramiko.config import SSH_PORT, SSHConfig
from paramiko.ssh_exception import NoValidConnectionsError
from scp import SCPClient, SCPException

from ..compat import Self, remove_file, rmtree
from ..config import load_config
from ..digest import key_to_relative
from ..interface import Key, Keys, MaybeLabels, Meta, PathOrStr, Value
from .interface import Location


class UnknownHostException(SSHException):
    pass


class SCP(Location):
    def __init__(self, hostname: str, root: PathOrStr, port: int = SSH_PORT, username: str = None, password: str = None,
                 key: Union[Path, Sequence[Path]] = ()):
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

    def __reduce__(self):
        return self.__class__, (self.hostname, self.root, self.port, self.username, self.password, self.key)

    @property
    def key_size(self):
        return sum(self.levels) if self.levels is not None else None

    @contextmanager
    def read(self, key: Key, return_labels: bool) -> ContextManager[Union[None, Value, Tuple[Value, MaybeLabels]]]:
        with self._connect() as scp:
            if not scp:
                yield
                return

            with tempfile.TemporaryDirectory() as temp_dir:
                source = Path(temp_dir) / 'source'
                try:
                    scp.get(str(self.root / key_to_relative(key, self.levels)), str(source), recursive=True)
                    if not source.exists():
                        yield None
                    else:
                        # TODO: legacy
                        if source.is_dir():
                            if return_labels:
                                yield source / 'data', None
                            else:
                                yield source / 'data'
                            rmtree(source)

                        else:
                            if return_labels:
                                yield source, None
                            else:
                                yield source
                            remove_file(source)

                except (SCPException, socket.timeout, SSHException):
                    yield None

    def read_batch(self, keys: Keys) -> Iterable[Tuple[Key, Union[None, Tuple[Value, MaybeLabels]]]]:
        with self._connect() as scp:
            if scp is None:
                for key in keys:
                    yield key, None
                return

            # TODO: add `retry` logic?
            with tempfile.TemporaryDirectory() as temp_dir:
                source = Path(temp_dir) / 'source'
                for key in keys:
                    try:
                        scp.get(str(self.root / key_to_relative(key, self.levels)), str(source), recursive=True)
                        if source.exists():
                            # TODO: legacy
                            if source.is_dir():
                                yield key, (source / 'data', None)
                                rmtree(source)

                            else:
                                yield key, (source, None)
                                remove_file(source)

                        else:
                            yield key, None

                    except (SCPException, socket.timeout, SSHException):
                        yield key, None

    def contents(self) -> Iterable[Tuple[Key, Self, Meta]]:
        # TODO
        return []

    @contextmanager
    def _connect(self) -> SCPClient:
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
            yield None
            return

        try:
            with SCPClient(self.ssh.get_transport()) as scp:
                if not self._get_config(scp):
                    yield None
                else:
                    yield scp

        finally:
            self.ssh.close()

    def _get_config(self, scp):
        try:
            if self.levels is None:
                with tempfile.TemporaryDirectory() as temp_dir:
                    temp = Path(temp_dir) / 'config.yml'
                    scp.get(str(self.root / 'config.yml'), str(temp))
                    remote_config = load_config(temp_dir)
                    self.hash, self.levels = remote_config.hash.build(), remote_config.levels

            return True

        except SCPException:
            return False
