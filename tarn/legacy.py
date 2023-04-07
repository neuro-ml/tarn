from .location import Level, Fanout


def StorageLevel(locations, write, replicate, name):
    return Level(Fanout(*locations), write=write, replicate=replicate, name=name)
