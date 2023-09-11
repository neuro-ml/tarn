[![codecov](https://codecov.io/gh/neuro-ml/tarn/branch/master/graph/badge.svg)](https://codecov.io/gh/neuro-ml/tarn)
[![pypi](https://img.shields.io/pypi/v/tarn?logo=pypi&label=PyPi)](https://pypi.org/project/tarn/)
![License](https://img.shields.io/github/license/neuro-ml/tarn)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/tarn)](https://pypi.org/project/tarn/)

A generic framework for key-value storage

# Install

```shell
pip install tarn
```

# Recipes

## A simple datalake

Let's start small and create a simple disk-based datalake. It will store various files, and the keys will be their
[sha256](https://en.wikipedia.org/wiki/SHA-2) digest:

```python
from tarn import HashKeyStorage

storage = HashKeyStorage('/path/to/some/folder')
# here `key` is the sha256 digest
key = storage.write('/path/to/some/file.png')
# now we can use the key to read the file at a later time
with storage.read(key) as value:
    # this will output something like Path('/path/to/some/folder/a0/ff9ae8987..')
    print(value.resolve())

# you can also store values directly from memory
# - either byte strings
key = storage.write(b'my-bytes')
# - or file-like objects
#  in this example we stream data from an url directly to the datalake
import requests

key = storage.write(requests.get('https://example.com').raw)
```

## Smart cache to disk

A really cool feature of `tarn` is [memoization](https://en.wikipedia.org/wiki/Memoization) with automatic invalidation:

```python
from tarn import smart_cache


@smart_cache('/path/to/storage')
def my_expensive_function(x):
    y = x ** 2
    return my_other_function(x, y)


def my_other_function(x, y):
    ...
    z = x * y
    return x + y + z
```

Now the calls to `my_expensive_function` will be automatically cached to disk.

But that's not all! Let's assume that `my_expensive_function` and `my_other_function` are often prone to change,
and we would like to invalidate the cache when they do. Just annotate these function with a decorator:

```python
from tarn import smart_cache, mark_unstable


@smart_cache('/path/to/storage')
@mark_unstable
def my_expensive_function(x):
    ...


@mark_unstable
def my_other_function(x, y):
    ...
```

Now any change to these functions, will cause the cache to invalidate itself!

## Other storage locations

We support multiple storage locations out of the box.

Didn't find the location you were looking for? Create an [issue](https://github.com/neuro-ml/tarn/issues).

### S3

```python
from tarn import HashKeyStorage, S3

storage = HashKeyStorage(S3('my-storage-url', 'my-bucket'))
```

### Redis

If your files are small, and you want a fast in-memory storage [Redis](https://redis.io/) is a great option

```python
from tarn import HashKeyStorage, RedisLocation

storage = HashKeyStorage(RedisLocation('localhost'))
```

### SFTP

```python
from tarn import HashKeyStorage, SFTP

storage = HashKeyStorage(SFTP('myserver', '/path/to/root/folder'))
```

### SCP

```python
from tarn import HashKeyStorage, SCP

storage = HashKeyStorage(SCP('myserver', '/path/to/root/folder'))
```

### Nginx

Nginx has an [autoindex](https://nginx.org/en/docs/http/ngx_http_autoindex_module.html#autoindex_format) option, that
allows to serve files and list directory contents. This is useful when you want to access files over http/https:

```python
from tarn import HashKeyStorage, Nginx

storage = HashKeyStorage(Nginx('https://example.com/storage'))
```

## Advanced

Here we'll show more specific (but useful!) use-cases

### Fanout

You might have several HDDs, and you may want to keep your datalake on both without creating a RAID array:

```python
from tarn import HashKeyStorage, Fanout

storage = HashKeyStorage(Fanout(
    '/mount/hdd1/lake',
    '/mount/hdd2/lake',
))
```

Now both disks are used, and we'll start writing to `/mount/hdd2/lake` after `/mount/hdd1/lake` becomes full.

You can even use other types of locations:

```python
from tarn import HashKeyStorage, Fanout, S3

storage = HashKeyStorage(Fanout(S3('server1', 'bucket1'), S3('server2', 'bucket2')))
```

Or mix and match them as you please:

```python
from tarn import HashKeyStorage, Fanout, S3

# write to s3, then start writing to HDD1 after s3 becomes full
storage = HashKeyStorage(Fanout(S3('server2', 'bucket2'), '/mount/hdd1/lake'))
```

### Lazy migration

Let's say you want to seamlessly replicate an old storage to a new location, but copy only the needed files first:

```python
from tarn import HashKeyStorage, Levels

storage = HashKeyStorage(Levels(
    '/mount/new-hdd/lake',
    '/mount/old-hdd/lake',
))
```

This will create something like a [cache hierarchy](https://en.wikipedia.org/wiki/Cache_hierarchy) with copy-on-read
behaviour. Each time we read a key, if we don't find it in `/mount/new-hdd/lake`, we read it from `/mount/old-hdd/lake`
and save a copy to `/mount/new-hdd/lake`.

### Cache levels

The same [cache hierarchy](https://en.wikipedia.org/wiki/Cache_hierarchy) logic can be used if you have a combination of
HDDs and SSD which will seriously speed up the reading:

```python
from tarn import HashKeyStorage, Levels, Level

storage = HashKeyStorage(Levels(
    Level('/mount/fast-ssd/lake', write=False),
    Level('/mount/slow-hdd/lake', write=False),
    '/mount/slower-nfs/lake',
))
```

The setup above is similar to the one we use in our lab:

- we have a slow but _huge_ NFS-mounted storage
- a faster but smaller HDD
- and a super fast but even smaller SSD

Now, we only write to the NFS storage, but the data gets lazily replicated to the local HDD and SSD to speed up the
reads.

### Caching small files to Redis

We can take this approach even further and use ultra fast in-memory storages, such as Redis:

```python
from tarn import HashKeyStorage, Levels, Small, RedisLocation

storage = HashKeyStorage(Levels(
    # max file size = 100KiB
    Small(RedisLocation('my-host'), max_size=100 * 1024),
    '/mount/hdd/lake',
))
```

Here we use `Small` - a wrapper that only allows small (<=100KiB in this case) files to be written to it.
In our experiments we observed a 10x speedup for reading small files.

## Composability

Because all the locations implement the same interface, you can start creating more complex storage logic specifically
tailored to your needs. You can make setups as crazy as you want!

```python
from tarn import HashKeyStorage, Levels, Fanout, RedisLocation, Small, S3, SFTP

storage = HashKeyStorage(Levels(
    Small(RedisLocation('my-host'), max_size=10 * 1024 ** 2),
    '/mount/fast-ssd/lake',

    Fanout(
        '/mount/hdd1/lake',
        '/mount/hdd2/lake',
        '/mount/hdd3/lake',

        # nested locations are not a problem!
        Levels(
            # apparently we want mirrored locations here
            '/mount/hdd3/lake',
            '/mount/old-hdd/lake',
        ),
    ),

    '/mount/slower-nfs/lake',

    S3('my-s3-host', 'my-bucket'),

    # pull missing files over sftp when needed
    SFTP('remove-host', '/path/to/remote/folder'),
))
```

# Acknowledgements

Some parts of our cache invalidation machinery were heavily inspired by
the [cloudpickle](https://github.com/cloudpipe/cloudpickle) project.
