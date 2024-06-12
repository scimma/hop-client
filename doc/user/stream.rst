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

    stream = Stream(until_eos=True)
    with stream.open("kafka://hostname:port/topic", "r") as s:
        for message in s:
             print(message.content)

The :code:`until_eos` option allows one to listen to messages until
the no more messages are available (EOS or end of stream). By default
the connection is kept open indefinitely.
This is to allow long-lived connections where one may set up a service
to process incoming GCNs, for example.

A common use case is to not specify any defaults ahead of time,
so a shorthand is provided for using one:

.. code:: python

    from hop import stream

    with stream.open("kafka://hostname:port/topic", "r") as s:
        for message in s:
             print(message.content)

A complete list of configurable options in :code:`Stream` are:

* :code:`auth`: A `bool` or :code:`auth.Auth` instance to provide authentication
* :code:`start_at`: The message offset to start at, by passing in an :code:`io.StartPosition`
* :code:`until_eos`: Whether to stop processing messages after an EOS is received

One doesn't have to use the context manager protocol (:code:`with` block)
to open up a stream as long as the stream is explicitly closed afterwards:

.. code:: python

    from hop import stream

    s = stream.open("kafka://hostname:port/topic", "r")
    for message in s:
         print(message.content)
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
             print(message.content, metadata.topic)

Anatomy of a Kafka URL
-----------------------

Both the CLI and python API take a URL that describes how to connect to various
Kafka topics, and takes the form:

.. code:: bash

   kafka://[username@]broker/[topic[,topic2[,...]]]

The broker takes the form :code:`hostname[:port]` and gives the URL to connect to a
Kafka broker. Optionally, a :code:`username` can be provided, which is used to select 
among available credentials to use when communicating with the broker. 
Finally, one can specify a number of topics to which to publish or subscribe.

Publishing to Multiple Topics
-------------------------------

A single stream object can be used to publish to multiple topics, and doing so uses resources
more efficiently by spawning fewer worker threads, opening fewer sockets, etc., than opening a
separate stream for each of several topics, but requires attention to one extra detail: When a
stream is opened for multiple topics, the topic must be specified when calling :code:`write()`,
in order to make unambiguous to which topic that particular message should be published:

.. code:: python

    from hop import stream

    with stream.open("kafka://hostname:port/topic1,topic2", "w") as s:
        s.write({"my": "message"}, topic="topic2")

In fact, when opening a stream for writing, it is not necessary for the target URL to contain
a topic at all; if it does not, the topic to which to publish must always be specified when
calling :code:`write()`.

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

    with stream.open("kafka://hostname:port/topic1", "r", "mygroup") as s:
        for message, metadata in s.read(metadata=True, autocommit=False):
             print(message.content, metadata.topic)
             s.mark_done(metadata)

Attaching Metadata to Messages
------------------------------

Apache Kafka supports headers to associate metadata with messages, separate from the message body,
and the hop python API supports this feature as well. Headers should generally be *small* and
ideally optional information; most of a message's content should be in its body.

Each header has a string key, and a binary or unicode value. A collection of headers may be provided
either as a dictionary or as a list of (key, value) tuples. Duplicate header keys are permitted;
the list representation is necessary to utilize this allowance.

It is important to note that Hopskotch reserves all header names starting with an underscore (``_``)
for internal use; users should not set their own headers with such names.

Sending messages with headers and viewing the headers attached to received messages can be done as
shown below:

.. code:: python

    from hop import stream

    with stream.open("kafka://hostname:port/topic1", "w") as s:
        s.write({"my": "message"}, headers={"priority": "1", "sender": "test"})
        s.write({"my": "other message"}, headers=[("priority", "2"), ("sender", "test")])

.. code:: python

    from hop import stream

    with stream.open("kafka://hostname:port/topic1", "r") as s:
        for message, metadata in s.read(metadata=True):
            print(message, metadata.headers)

Standard Headers
----------------

The Hop client produces and uses certain message headers automatically. It is designed so that each
header is intended to be optional, in the sense that messages lacking these headers can still be
processed, but if a header is missing, functionality based on it may not be available. Headers
currently automatically produced and used are:

* :code:`_id`: The value of this header is a unique ID intended to allow referring to the specific
  message without requiring context like its position within a Kafka topic. Message IDs are
  currently generated as version 4 `RFC 4122 <https://datatracker.ietf.org/doc/html/rfc4122.html>`_
  UUIDs. If the message ID header is missing, other users may not be able to send messages
  which refer to the message, and systems which store messages may not be able to look it up
  directly.
* :code:`_sender`: The value of this header is the username associated with the credential used to
  send the message, if any.
* :code:`_test`: The presence of this header, with any value, should be interpreted to mean that the
  message is a test, whose content may be safely ignored, or should otherwise not
  necessarily be acted upon normally.
* :code:`_format`: The value of this header is a UTF-8 string which is used to identify which
  message model should be used to decode the message content. If the format header is missing,
  an attempt will be made to decode the message content as JSON for backwards
  compatibility with old client versions, and if it is not valid JSON the message content
  will be left raw (treated as a `Blob`).

Because these header values are attached to messages by the publishing client, subscribers and
systems receiving messages should be careful about the degree to which they trust the header values.
For example, an ill-behaved publisher might re-use a message ID, or set an incorrect sender username.
In most cases, however, due to the authentication and authorization systems enforced by the Kafka
broker, subscribers receiving a message can generally trust its header values to the same extent
that they trust the data in the message body, based on the entities they know are authorized to
publish to the topic on which the message appears.