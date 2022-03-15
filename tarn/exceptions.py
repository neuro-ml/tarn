class StorageCorruption(OSError):
    """
    Denotes various problems with disk-based storage or persistent cache
    """


class StorageError(Exception):
    pass


class WriteError(StorageError):
    pass


class ReadError(StorageError):
    pass
