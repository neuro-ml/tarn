from .disk_dict import DiskDict
from .fanout import Fanout
from .interface import Location, Writable
from .levels import Level, Levels
from .nginx import Nginx
from .redis import RedisLocation
from .s3 import S3
from .ssh import SCP, SFTP
from .small import Small

# TODO: deprecated
SmallLocation = Small
