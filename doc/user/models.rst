================
Message Formats
================

.. contents::
   :local:

The hop client provides a few in-memory representations for common
message types for easy access to various message properties, as well
as loading messages from their serialized forms or from disk. These
message formats, or models, can be sent directly to an open :code:`Stream`
to provide seamless serialization of messages through Hopskotch.

Structured Messages
--------------------

Currently, the structured messages available through the hop client
are :code:`VOEvent` and :code:`GCNCircular`.
To give an example of its usage:


.. code:: python

    from hop import Stream
    from hop.auth import load_auth
    from hop.models import VOEvent

    xml_path = "/path/to/voevent.xml"
    voevent = VOEvent.load_file(xml_path)

    stream = Stream(auth=load_auth())
    with stream.open("kafka://hostname:port/topic", "w") as s:
        s.write(voevent)

Unstructured Messages
-----------------------

Unstructured (or less structured messages) can be sent directly to an open
:code:`Stream` instance. Any python object that can be JSON
serialized can be sent. Examples include a dictionary, a
string, and a list. At an even more raw level, bytes objects can be sent
without any further encoding or interpretation.

On the more structured end of the spectrum, the hop client understands some
general data formats, and can automatically encode and decode them. These
include JSON with the :code:`JSONBlob` class and Apache Avro with the
:code:`AvroBlob` class. Using :code:`JSONBlob` is equivalent to simply
writing an unstructured but JSON-encodable python object. :code:`AvroBlob`
supports efficiently including bytes subobjects, as well as schemas. If no
schema is supplied, it will create a schema to describe the object(s) it is
given, but a deliberately designed schema may also be used.


.. code:: python

    from hop import Stream
    from hop.auth import load_auth
    from hop.models import JSONBlob, AvroBlob
    import fastavro

    stream = Stream(auth=load_auth())
    with stream.open("kafka://hostname:port/topic", "w") as s:

        # Writing simple, unstructured messages
        s.write("a string message")
        s.write(["some", "data", "with", "numbers:", 5, 6, 7])
        s.write({"priority": 1, "payload": "data"})
        s.write(b'\x02Binary data\x1DMessage ends\x03')

        # Explicitly writing a partially-structured message as JSON
        s.write(JSONBlob({"priority": 1, "payload": "data"}))

        # Write an Avro message with an ad-hoc schema
        # Avro may contain arbitrarily many records,
        # so it always expects a list or records to be written
        s.write(AvroBlob([{"priority": 1, "payload": b'\x02Binary data\x03'}]))

        # Write an Avro message with a specific schema
        schema = fastavro.load_schema("my_schema.avsc")
        s.write(AvroBlob([{priority: 1, payload: b'\x02Binary data\x03'}],
                         schema=schema))


All unstructured messages are unpacked by the hop client back into message
model objects containing python objects equivalent to what was sent when they
are read from a stream. The decoded objects are available from each of the
unstructured message model types as :code:`content` property. Some model types
also make additional information available, for example, the :code:`AvroBlob`
also has a :code:`schema` property which contains the schema with which the
message was sent.

Register External Message Models
---------------------------------

Sometimes it may be useful to use custom structured messages that aren't currently
available in the stock client. For instance, sending specialized messages between
services that are internal to a specific observatory. The hop client provides a
mechanism in which to register custom message types that are discoverable within
hop when publishing and subscribing for your own project. This requires creating
an external python library and setting up an entry point so that hop that discover
it upon importing the client.

There are three steps involved in creating and registering a custom message model:

#. Define the message model.
#. Register the message model.
#. Set up an entry point within your package.

Define a message model
^^^^^^^^^^^^^^^^^^^^^^^

To do this, you need to define a dataclass that subclasses :code:`hop.models.MessageModel`
and implement functionality to load your message mode via
the :code:`load()` class method. As an example, assuming the message is represented as
JSON on disk:

.. code:: python

    from dataclasses import dataclass
    import json

    from hop.models import MessageModel

    @dataclass
    class Donut(MessageModel):

        category: str
        flavor: str
        has_filling: bool

        @classmethod
        def load(cls, input_):
            # input_ is a file object
            if hasattr(donut_input, "read"):
                donut = json.load(input_)
            # serialized input_
            else:
                donut = json.loads(input_)

            # unpack the JSON dictionary and return the model
            return cls(**donut)

For more information on dataclasses, see the `Python Docs <https://docs.python.org/3/library/dataclasses.html>`_.

Register a message model
^^^^^^^^^^^^^^^^^^^^^^^^^

Once you have defined your message model, registering the message model involves
defining a function with the :code:`hop.plugins.register` decorator with key-value
pairs mapping a message model name and the model:

.. code:: python

    from hop import plugins

    ...

    @plugins.register
    def get_models():
        return {
            "donut": Donut,
        }


Set up entry points within your package
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

After registering your model, you'll need to set up an entry point to your package
named :code:`hop_plugin` as that entry point is explicitly used to auto-discover
new plugins. The module used for the entry point is wherever you registered your
model.

Setting up entry points may be different depending on how your package is set up.
Below we'll give an example for setuptools and setup.py. In setup.py:

.. code:: python

    from setuptools import setup

    ...

    setup(
        ...

        entrypoints = {"hop_plugin": ["donut-plugin = my.custom.module"]}
    )


Some further resources on entry points:

* `https://setuptools.readthedocs.io/en/latest/setuptools.html#dynamic-discovery-of-services-and-plugins <https://setuptools.readthedocs.io/en/latest/setuptools.html#dynamic-discovery-of-services-and-plugins>`_
