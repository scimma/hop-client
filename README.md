Hop Client
=============

![](https://github.com/scimma/hop-client/workflows/build/badge.svg)
[![codecov](https://codecov.io/gh/scimma/hop-client/branch/master/graph/badge.svg)](https://codecov.io/gh/scimma/hop-client)

|              |        |
| ------------ | ------ |
| **Docs:**    | https://hop-client.readthedocs.io/en/stable/  |

**hop-client** is a pub-sub client library for Multimessenger Astrophysics.

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

## Quickstart

By default, authentication is enabled, reading in configuration settings
from `config.toml`. The path to this configuration can be found by running
`hop auth locate`. One can initialize this configuration with default
settings by running `hop auth setup`. To disable authentication in the CLI
client, one can run `--no-auth`.

### Command Line Interface

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

### Python API

Publish messages:

Using the python API, we can publish various types of messages, including
structured messages such as GCN Circulars and VOEvents:

```python
from hop import stream
from hop.models import GCNCircular

# read in a GCN circular
with open("path/to/circular.gcn3", "r") as f:
    circular = GCNCircular.load(f)

with stream.open("kafka://hostname:port/topic", "w") as s:
    s.write(circular)
```

In addition, we can also publish unstructured messages as long as they are
JSON serializable:

```python
from hop import stream

with stream.open("kafka://hostname:port/topic", "w") as s:
    s.write({"my": "message"})
```

By default, authentication is enabled for the Hop broker, reading in configuration
settings from `config.toml`. In order to modify various authentication options, one
can configure a `Stream` instance and pass in an `Auth` instance with credentials:

```python
from hop import Stream
from hop.auth import Auth

auth = Auth("my-username", "my-password")
stream = Stream(auth=auth)

with stream.open("kafka://hostname:port/topic", "w") as s:
    s.write({"my": "message"})
```

To explicitly disable authentication one can set `auth` to `False`.

Consume messages:


```python
from hop import stream

with stream.open("kafka://hostname:port/topic", "r") as s:
    for message in s:
         print(message)
```

This will listen to the Hop broker, listening to new messages and printing them to
stdout as they arrive until there are no more messages in the stream.
By default, this will only process new messages since the connection was opened.
The `start_at` option lets you control where in the stream you can start listening
from. For example, if you'd like to listen to all messages stored in a topic, you can do:

```python
from hop import Stream
from hop.io import StartPosition

stream = Stream(start_at=StartPosition.EARLIEST)

with stream.open("kafka://hostname:port/topic", "r") as s:
    for message in s:
         print(message)
```


## Development

A Makefile is provided to ease in testing, deployment and generating documentation.

A list of commands can be listed with `make help`.

In addition, two extras are provided when installing the hop client that installs
the required test and documentation libraries:

* dev: dependencies required for testing, linting and packaging
* docs: dependencies required for building documentation

Assuming you've cloned the repository and are in the project's root directory, you can
install hop-client alongside all the required development dependencies by running:

```
pip install .[dev,docs]
```

### Releases

To mark a new version, use Github tags to mark your commit with a [semver](https://semver.org/) version:
```
git tag v0.0.1
```

To release a new version and upload to package repositories, push your tag after pushing your commit:
```
git push --tags
```
