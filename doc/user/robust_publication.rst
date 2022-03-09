==================
Robust Publication
==================

.. contents::
   :local:

The RobustProducer
------------------

In some situations, it may be critical to ensure that messages are sent to the Kafka broker, even when the
intervening network may be unreliable, the sending process may be killed unexpectedly, and so on.
The :class:`RobustProducer <hop.robust_publisher.RobustProducer>` object extends the capabilities of the 
simple :class:`Producer <hop.io.Producer>` to provide this functionality. 

The two main mechanisms used by the :class:`RobustProducer <hop.robust_publisher.RobustProducer>` are to 
maintain a local journal of messages which are queued to be sent or are in flight, and to listen for
confirmation messages from the Kafka broker that messages have been received.
The use of receipt confirmations enables the resending of messages which are lost in the network or if the broker
fails unexpectedly, while use of the journal ensures that even if the sending program is stopped suddenly, 
it can resend any messages whose receipt was not yet confirmed. 
Implications of this are that local disk space is required for the message journal (and the amount of
space used will be at least that of the sum of sizes of all messages in flight at the same time), and that
at-least-once delivery is guaranteed, but that in providing that guarantee, messages may be duplicated.
For example, duplication of messages on the broker will occur if the producer sends the mesage, but the broker's
confirmation is lost in the network, so the producer is forced to assume that the message did not go
through and resends it. 
Clients should be prepared to handle duplicate messages appropriately. 

Usage
-----

The simplest way to use the :class:`RobustProducer <hop.robust_publisher.RobustProducer>` is as a context
manager:

.. code:: python

    from hop.robust_publisher import RobustProducer

    with RobustProducer("kafka://hostname:port/topic") as publisher:
        for message in messages:
            publisher.write(message)

To control the location where the message journal is stored, one may set the :code:`journal_path` option
when constructing the :class:`RobustProducer <hop.robust_publisher.RobustProducer>`; the default is
:code:`"publisher.journal"` which will place it the script's current working directory. 

Message sending is asynchronous, so
:meth:`RobustProducer.write <hop.robust_publisher.RobustProducer.write>` will return almost immediately,
as it only queues the message for sending. 
The :class:`RobustProducer <hop.robust_publisher.RobustProducer>` blocks internally until all messages are
successfully sent, so there can be a noticeable delay after all messages have been queued while they
complete sending. 
In the event of a network or broker failure, this delay may extend indefinitely. 

The :meth:`RobustProducer constructor <hop.robust_publisher.RobustProducer.__init__>` also
accepts an :code:`auth` argument for specifying the
credentials with which it should connect, and will pass through any extra keyword arguments to
:meth:`io.Stream.open <hop.io.Stream.open>`. 

For more advanced uses, :class:`RobustProducer <hop.robust_publisher.RobustProducer>` can also be used
directly without being treated as a context manager:

.. code:: python

    from hop.robust_publisher import RobustProducer

    publisher = RobustProducer("kafka://hostname:port/topic")
    publisher.start()

    #. . .
    publisher.write(some_message)
    
    #. . .
    publisher.stop()

When used in this way, it is necessary to call
:meth:`RobustProducer.start <hop.robust_publisher.RobustProducer.start>`
before sending any messages, and :meth:`RobustProducer.stop <hop.robust_publisher.RobustProducer.stop>`
after all messages have been sent to shut down the
:class:`RobustProducer <hop.robust_publisher.RobustProducer>`'s internal background worker thread.
It is important to note that the user should *not* call
:meth:`RobustProducer.run <hop.robust_publisher.RobustProducer.run>`, as this method is exposed only as a
part of the python :class:`threading.Thread` interface, and will block whatever thread calls it,
indefinitely. 
Once stopped, a :class:`RobustProducer <hop.robust_publisher.RobustProducer>` object cannot be restarted. 

Miscellaneous Details
---------------------

The message journal is intended to protect against disruption of the sending program, but at this time
does not include meaningful protection against sudden failure of the machine on which the program is
running; in particular, it does not ensure that data written to it is definitely flushed through
filesystem or hardware caching layers. 
As a result, issues like power failures can lead to data loss. 
The journal does contain checksumming and other sanity checking which enable detecting most forms of data
corruption, although truncation of the journal exactly at a boundary between entries currently cannot be
detected.
Currently, corruption of the journal will trigger an error and block (re)starting the
:class:`RobustProducer <hop.robust_publisher.RobustProducer>`.

Messages are written to the journal essentially in plain text, so users whose data is sensitive should
take into account that the journal file must be suitably protected. 

Currently, :meth:`RobustProducer.write <hop.robust_publisher.RobustProducer.write>` takes over the
:code:`delivery_callback` option for :meth:`Producer.write <hop.io.Producer.write>` for its own use, so
end users are not able to register their own delivery callback handlers. 
