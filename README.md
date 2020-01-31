SCiMMA Client
=============

![](https://github.com/scimma/client_library/workflows/build/badge.svg)

**scimma-client** is a pub-sub client library for Multimessenger Astrophysics.

## Quickstart

Publish a GCN to Kafka:

```
scimma publish -b kafka://hostname:port/gcn mygcn.gcn3
```

## Installation

You can install scimma-client either via pip, conda, or from source.

To install with pip:

```
pip install -U scimma-client
```

To install with conda, you must use the channel from the SCiMMA Anaconda organization:

```
conda install --channel scimma scimma-client
```

To install from source:

```
tar -xzf scimma-client-x.y.z.tar.gz
cd scimma-client-x.y.z
python setup.py install
```

## Development

A Makefile is provided to ease in testing, deployment and generating documentation.

A list of commands can be listed with `make help`.

In addition, two extras are provided when installing the scimma client that installs
the required test and documentation libraries:

```
pip install -U scimma-client[dev,docs]
```
