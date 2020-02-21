==========
Quickstart
==========

.. contents::
   :local:

Publish a GCN to Kafka:

.. code:: bash

    scimma publish -b kafka://hostname:port/gcn mygcn.gcn3


An example RFC 822 formatted GCN circular (:code:`example.gcn3`) is provided in :code:`tests/data`.

Client `configuration <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md>`_
properties can be passed to :code:`scimma publish` via :code:`-X property=value` or in a configuration
file specified by :code:`-F <config-file>`, mimicking the behavior of :code:`kafkacat`. This can be
used to connect to a Kafka broker with SSL authentication enabled, for example.
