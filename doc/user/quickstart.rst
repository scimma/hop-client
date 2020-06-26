==========
Quickstart
==========

.. contents::
   :local:


Using the CLI
-------------

Publish a message
^^^^^^^^^^^^^^^^^

.. code:: bash

    hop publish kafka://hostname:port/gcn -f circular example.gcn3

Two example messages (an RFC 822 formatted GCN circular (:code:`example.gcn3`) and a VOEvent 2.0
schema xml (:code: `example_voevent.xml`)) are provided in :code:`tests/data`.

Client `configuration <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md>`_
properties can be passed to :code:`hop publish` via :code:`-X property=value` or in a configuration
file specified by :code:`-F <config-file>`, mimicking the behavior of :code:`kafkacat`. This can be
used to connect to a Kafka broker with SSL authentication enabled, for example.

Consume a message
^^^^^^^^^^^^^^^^^

.. code:: bash

    hop subscribe kafka://hostname:port/gcn -e

Configuration properties can be passed in a manner identical to :code:`hop publish` above.


Reading messages from stream
----------------------------

The hop client supports a python-based API for reading messages from a stream, as follows:

.. code:: python

    from hop import stream

    with stream.open("kafka://hostname:port/topic", "r", format="json") as s:
        for idx, msg in s:
             print(msg)

This block will hang forever, listening to new messages and processing them as they arrive.
By default, this will only process new messages since the connection was opened. The :code:`start_at`
option lets you control where in the stream you can start listening from. For example,
if you'd like to listen to all messages stored in a topic, you can do:

.. code:: python

    with stream.open("kafka://hostname:port/topic", "r", format="json", start_at="latest") as s:
        for idx, msg in s:
             print(msg)

Writing messages to stream
--------------------------

We can also publish messages to a topic, as follows:

.. code:: python

    from hop import stream

    with stream.open("kafka://hostname:port/topic", "w", format="json") as s:
        s.write({"my": "message"})
