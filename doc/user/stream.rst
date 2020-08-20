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

One doesn't have to use the context manager protocol (:code:`with` block)
to open up a stream as long as the stream is explicitly closed afterwards:

.. code:: python

    from hop import stream

    s = stream.open("kafka://hostname:port/topic", "r")
    for message in s:
         print(message)
    s.close()

So far, all examples have shown the iterator interface for reading messages from an open
stream. But one can instead call :code:`s.read()` directly or in the case of more specialized
workflows, may make use of extra keyword arguments to configure an open stream. For example,
the :code:`metadata` option allows one to retrieve Kafka message metadata as well
as the message itself, such as the Kafka topic, key, timestamp and offset. This may
be useful in the case of listening to multiple topics at once:

.. code:: python

    from hop import stream

    with stream.open("kafka://hostname:port/topic1,topic2", "r") as s:
        for message, metadata in s.read(metadata=True):
             print(message, metadata.topic)

Anatomy of a Kafka URL
-----------------------

Both the CLI and python API take a URL that describes how to connect to various
Kafka topics, and takes the form:

.. code:: bash

   kafka://[groupid@]broker/topic[,topic2[,...]]

The broker takes the form :code:`hostname[:port]` and gives the URL to connect to a
Kafka broker. Optionally, a :code:`groupid` is provided which is used to keep track
of which messages have been read from a topic with a given group ID. This allows a long-lived
process reading messages to pick up where they left off after a restart, for example.
Finally, one can publish to a topic or subscribe to one or more topics to consume messages
from.

Committing Messages Manually
------------------------------

By default, messages that are read in by the stream are marked as read immediately after
returning them from an open stream instance for a given group ID. This is suitable for most cases,
but some workflows have more strict fault tolerance requirements and don't want to lose
messages in the case of a failure while processing the current message. We can instead commit
messages after we are done processing them so that in the case of a failure, a process that is
restarted can get the same message back and finish processing it before moving on to the next.
This requires returning broker-specific metadata as well as assigning yourself to a specific group ID.
A workflow to do this is shown below:

.. code:: python

    from hop import stream

    with stream.open("kafka://mygroup@hostname:port/topic1", "r") as s:
        for message, metadata in s.read(metadata=True, autocommit=False):
             print(message, metadata.topic)
             s.mark_done(metadata)
