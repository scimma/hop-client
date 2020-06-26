Hop Client
=============

![](https://github.com/scimma/hop-client/workflows/build/badge.svg)

**hop-client** is a pub-sub client library for Multimessenger Astrophysics.

## Quickstart

Publish a GCN to Kafka:

```
hop publish kafka://hostname:port/gcn mygcn.gcn3
```

Subscribe to the earliest offset of a Kafka topic and print to stdout:
```
hop subscribe kafka://hostname:port/gcn -e
```

An example RFC 822 formatted GCN circular (`example.gcn3`) is provided in
`tests/data`.

Client [configuration](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
properties can be passed to `hop publish` via `-X property=value` or in a configuration
file specified by `-F <config-file>`, mimicking the behavior of `kafkacat`. This can be
used to connect to a Kafka broker with SSL authentication enabled, for example.

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
