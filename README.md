Hop Client
=============

![](https://github.com/scimma/hop-client/workflows/build/badge.svg)
[![codecov](https://codecov.io/gh/scimma/hop-client/branch/master/graph/badge.svg)](https://codecov.io/gh/scimma/hop-client)

|              |        |
| ------------ | ------ |
| **Docs:**    | https://hop-client.readthedocs.io/en/stable/  |

**hop-client** is a pub-sub client library for Multimessenger Astrophysics.

## Quickstart

By default, authentication is enabled, reading in configuration settings
from `config.toml`. The path to this configuration can be found by running
`hop auth locate`. One can initialize this configuration with default
settings by running `hop auth setup`. To disable authentication in the CLI
client, one can run `--no-auth`.

Publish a message:

```
hop publish kafka://hostname:port/gcn -f CIRCULAR example.gcn3
```

Example messages are provided in `tests/data` including:
* A GCN circular (`example.gcn3`)
* A VOEvent (`example_voevent.xml`)


Consume messages:

```
hop subscribe kafka://hostname:port/gcn -s EARLIEST
```

This will read messages from the gcn topic from the earliest offset
and read messages until an end of stream (EOS) is received.

## Installation

You can install hop either via pip, conda, or from source.

To install with pip:

```
pip install -U hop-client
```

To install with conda, you must use the channel from the SCiMMA Anaconda organization:

```
conda install --channel scimma hop-client
```

To install from source:

```
tar -xzf hop-client-x.y.z.tar.gz
cd hop-client-x.y.z
python setup.py install
```

## Development

A Makefile is provided to ease in testing, deployment and generating documentation.

A list of commands can be listed with `make help`.

In addition, two extras are provided when installing the scimma client that installs
the required test and documentation libraries:

```
pip install -U hop-client[dev,docs]
```

To mark a new version, use Github tags to mark your commit with a [semver](https://semver.org/) version:
```
git tag v0.0.1
```

To release a new version and upload to package repositories, push your tag after pushing your commit:
```
git push --tags
```
