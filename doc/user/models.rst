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

Currently, the structured messages available are :code:`VOEvent` and
:code:`GCNCircular`. To give an example of its usage:


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

Unstructured messages can be sent directly to an open :code:`Stream` instance
and will be serialized appropriately. Any python object that can be JSON
serialized can be sent. Examples include a dictionary, a byte64 encoded
string, and a list.
