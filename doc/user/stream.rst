==========
Stream
==========

.. contents::
   :local:

The Stream Object
-----------------

The Stream object allows a user to connect to a Kafka broker and read
in a variety of alerts, such as GCN circulars. It also allows one to
specify default settings used across all streams opened from the Stream
instance.

Let's open up a stream and show the Stream object in action:

.. code:: python

    from hop import Stream

    stream = Stream(format="json")
    with stream.open("kafka://hostname:port/topic", "r") as s:
        for idx, msg in s:
             print(msg)

A common use case is to not specify any defaults, so a shorthand is
provided for using one:

.. code:: python

    from hop import stream

    with stream.open("kafka://hostname:port/topic", "r") as s:
        for _, msg in s:
             print(msg)

You can also configure the open stream handle with various options,
including a timeout, a progress bar, and a message limit:

.. code:: python

    with stream.open("kafka://hostname:port/topic", "r") as s:
        for _, msg in s(timeout=10, limit=20):
             print(msg)
