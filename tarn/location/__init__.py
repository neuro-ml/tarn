from .disk_dict import DiskDict
from .fanout import Fanout
from .interface import Location, ReadOnly
from .levels import Level, Levels
from .nginx import Nginx
from .redis import RedisLocation
from .s3 import S3
from .small import Small
from .ssh import SCP, SFTP

# TODO: deprecated
SmallLocation = Small
