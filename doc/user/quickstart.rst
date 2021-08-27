==========
Quickstart
==========

.. contents::
   :local:


Using the CLI
-------------

By default, authentication is enabled, reading in credentials
from :code:`auth.toml`. The path to this configuration can be found by running
:code:`hop auth locate`. One can initialize this configuration with default
settings by running :code:`hop auth add`. To disable authentication in the CLI
client, one can use the :code:`--no-auth` option.

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
and read messages as they arrive. By default this will listen to
messages until the user stops the program (Ctrl+C to stop).

View Available Topics
^^^^^^^^^^^^^^^^^^^^^

.. code:: bash

    hop list-topics kafka://hostname:port/

This will list all of the topics on the given server which you are currently
authorized to read or write. 

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

By default, authentication is enabled for the Hop broker, reading in configuration
settings from :code:`config.toml`. In order to modify various authentication options, one
can configure a :code:`Stream` instance and pass in an :code:`Auth` instance with credentials:

.. code:: python

    from hop import Stream
    from hop.auth import Auth

    auth = Auth("my-username", "my-password")
    stream = Stream(auth=auth)

    with stream.open("kafka://hostname:port/topic", "w") as s:
        s.write({"my": "message"})

To explicitly disable authentication, one can set :code:`auth` to :code:`False`.

Consume messages
^^^^^^^^^^^^^^^^^

One can consume messages through the python API as follows:

.. code:: python

    from hop import stream

    with stream.open("kafka://hostname:port/topic", "r") as s:
        for message in s:
             print(message)

This will listen to the Hop broker, listening to new messages and printing them to
stdout as they arrive.
By default, this will only process new messages since the connection was opened.
The :code:`start_at` option lets you control where in the stream you can start listening
from. For example, if you'd like to listen to all messages stored in a topic, you can do:

.. code:: python

    from hop import stream
    from hop.io import StartPosition

    stream = Stream(start_at=StartPosition.EARLIEST)

    with stream.open("kafka://hostname:port/topic", "r") as s:
        for message in s:
             print(message)

