A hashmap-based storage on local and remote disks

# Install

The simplest way is to get it from PyPi:

```shell
pip install tarn
```

Or if you want to try the latest version from GitHub:

```shell
git clone https://github.com/neuro-ml/tarn.git
cd tarn
pip install -e .

# or let pip handle the cloning:
pip install git+https://github.com/neuro-ml/tarn.git
```

# Acknowledgements

Some parts of our automatic cache invalidation machinery were heavily inspired by
the [cloudpickle](https://github.com/cloudpipe/cloudpickle) project.
