class StorageCorruption(OSError):
    pass


class StorageError(Exception):
    pass


class WriteError(StorageError):
    pass


class ReadError(StorageError):
    pass
