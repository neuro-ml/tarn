class StorageCorruption(OSError):
    pass


class StorageError(Exception):
    pass


class WriteError(StorageError):
    pass


class ReadError(StorageError):
    pass


class SerializerError(Exception):
    """The current serializer can't work with the given object/path"""


class DeserializationError(ReadError):
    """Something is wrong with the data being deserialized"""


class CollisionError(WriteError):
    """Values doesn't match"""
