==========
Quickstart
==========

.. contents::
   :local:


Using the CLI
-------------

By default, authentication is enabled, reading in configuration settings
from :code:`auth.conf`. The path to this configuration can be found by running
:code:`hop auth locate`. One can initialize this configuration with default
settings by running :code:`hop auth setup`.Also, using `hop auth setup -c <CREDENTIALS_FILE>`, 
one can initialize the configuration file using the credentials file.
To disable authentication in the CLI
client, one can run :code:`--no-auth`.

Publish messages
^^^^^^^^^^^^^^^^^

.. code:: bash

    hop publish kafka://hostname:port/gcn -f CIRCULAR example.gcn3

Example messages are provided in :code:`tests/data` including:

* A GCN circular (:code:`example.gcn3`)
* A VOEvent (:code:`example_voevent.xml`)

Consume messages
^^^^^^^^^^^^^^^^^

.. code:: bash

    hop subscribe kafka://hostname:port/gcn -s EARLIEST

This will read messages from the gcn topic from the earliest offset
and read messages until an end of stream (EOS) is received.

Using the Python API
----------------------

Publish messages
^^^^^^^^^^^^^^^^^

Using the python API, we can publish various types of messages, including
structured messages such as GCN Circulars and VOEvents:

.. code:: python

    from hop import stream
    from hop.models import GCNCircular

    # read in a GCN circular
    with open("path/to/circular.gcn3", "r") as f:
        circular = GCNCircular.load(f)

    with stream.open("kafka://hostname:port/topic", "w") as s:
        s.write(circular)

In addition, we can also publish unstructured messages as long as they are
JSON serializable:

.. code:: python

    from hop import stream

    with stream.open("kafka://hostname:port/topic", "w") as s:
        s.write({"my": "message"})

By default, authentication is enabled for the Hop broker. In order to authenticate, one
can pass in an :code:`Auth` instance with credentials:

.. code:: python

    from hop import stream
    from hop.auth import Auth

    auth = Auth("my-username", "my-password")

    with stream.open("kafka://hostname:port/topic", "w", auth=auth) as s:
        s.write({"my": "message"})

A convenience function is also provided to read in auth configuration in the same way
as in the CLI client:

.. code:: python

    from hop import stream
    from hop.auth import load_auth

    with stream.open("kafka://hostname:port/topic", "w", auth=load_auth()) as s:
        s.write({"my": "message"})

Consume messages
^^^^^^^^^^^^^^^^^

One can consume messages through the python API as follows:

.. code:: python

    from hop import stream

    with stream.open("kafka://hostname:port/topic", "r") as s:
        for message in s:
             print(message)

This will listen to the Hop broker, listening to new messages and printing them to
stdout as they arrive until there are no more messages in the stream.
By default, this will only process new messages since the connection was opened.
The :code:`start_at` option lets you control where in the stream you can start listening
from. For example, if you'd like to listen to all messages stored in a topic, you can do:

.. code:: python

    from hop import stream
    from hop.io import StartPosition

    with stream.open("kafka://hostname:port/topic", "r", start_at=StartPosition.EARLIEST) as s:
        for message in s:
             print(message)

