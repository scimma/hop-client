================
Streaming
================

.. contents::
   :local:

The Stream Object
-----------------

The :code:`Stream` object allows a user to connect to a Kafka broker and read
in a variety of alerts, such as GCN circulars. It also allows one to
specify default settings used across all streams opened from the Stream
instance.

Let's open up a stream and show the :code:`Stream` object in action:

.. code:: python

    from hop import Stream

    stream = Stream(persist=True)
    with stream.open("kafka://hostname:port/topic", "r") as s:
        for message in s:
             print(message)

The :code:`persist` option allows one to listen to messages forever
and keeps the connection open after an end of stream (EOS) is received.
This is to allow long-lived connections where one may set up a service
to process incoming GCNs, for example.

A common use case is to not specify any defaults ahead of time,
so a shorthand is provided for using one:

.. code:: python

    from hop import stream

    with stream.open("kafka://hostname:port/topic", "r") as s:
        for message in s:
             print(message)

A complete list of configurable options in :code:`Stream` are:

* :code:`auth`: A `bool` or :code:`auth.Auth` instance to provide authentication
* :code:`start_at`: The message offset to start at, by passing in an :code:`io.StartPosition`
* :code:`persist`: Whether to keep a long-live connection to the client beyond EOS

In addition, :code:`stream.open` provides an option to retrieve Kafka message metadata as well
as the message itself, such as the Kafka topic, key, timestamp and offset. This may
be useful in the case of listening to multiple topics at once:

.. code:: python

    from hop import stream

    with stream.open("kafka://hostname:port/topic1,topic2", "r", metadata=True) as s:
        for message, metadata in s:
             print(message, metadata.topic)
