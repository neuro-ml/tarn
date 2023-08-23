def identity(x):
    return x


def scope():
    def identity(x):
        return x

    return identity


nested_identity = scope()


def calls_counter(x):
    calls_counter.counter += 1
    return x


calls_counter.counter = 0
