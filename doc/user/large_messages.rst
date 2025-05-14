=====================
Large Message Support
=====================

.. contents::
   :local:

Introduction
------------

Apache Kafka is generally intended to support high-throughput and low-latency of relatively small messages. 
As such it typically has a relatively modest size limit on the maximum size of an individual message,
often one or a few megabytes, and performance can be degraded by increasing this limit arbitrarily.
It can nonetheless be extremely useful to be able to use a single system to transmit messages which are sometimes much larger. 
To provide this flexibility, :code:`hop-client` implements an extension which, when used in conjunction with a suitably configured broker, can transparently handle large messages via a secondary communication channel which is configured for a larger maximum size. 
By default, this mechanism will be used automatically if the broker supports it, with publishing clients uploading data to the 'offload' API endpoint, and subscribing clients downloading it again when appropriate. 

.. _enable_disable:

Enabling or Disabling
---------------------

If desired, :code:`hop-client` allows both automatic offloading of large messages and automatic fetching of offloaded message data to be independently enabled or disabled; both features are enabled by default.

These can be globally/permanently configured via using the :code:`hop configure set` command to set the parameters :code:`automatic_offload` and :code:`fetch_external`, respectively. 

These behaviors can also be controlled in a specific context by setting the :code:`HOP_AUTOMATIC_OFFLOAD` or :code:`HOP_FETCH_EXTERNAL` environment variables. 

Finally, they can be programmatically overridden for individual cases by setting the :code:`automatic_offload` or :code:`fetch_external` keyword arguments to :code:`hop.io.stream` (or to :code:`hop.io.Producer` and :code:`hop.io.Consumer`, respectively). 

Protocol Details
----------------

Knowledge of these details should not be required for users of this feature, but is relevant for anyone seeking to set up a compatible broker. 

Discovery Mechanism
^^^^^^^^^^^^^^^^^^^

Clients find out whether a broker supports message offloading by reading from a specific topic, :code:`sys.metadata`. 
The client will attempt to read the most recent message on this topic, and if that message is a JSON object containing a key :code:`LargeMessageUploadEndpoint`, whose value is an HTTP(S) URL, this will be assumed to be the endpoint for a compatible API (as described below) which shares authentication and authorization with the Kafka broker. 

Authentication
^^^^^^^^^^^^^^

The offload API should generally support the same authentication as the broker with which it is associated, as the client will use the same credentials to authenticate with both. 
Specifically, when the broker uses SCRAM authentication, the offload API should use HTTP SCRAM Authentication as described in `RFC 7804 <https://datatracker.ietf.org/doc/html/rfc7804>`_. 

.. note::
  Although :code:`hop-client` supports OIDC authentication with Apache Kafka, 
  it does not yet support it for authenticating with an offload API.

Authorization
^^^^^^^^^^^^^

The offload API is expected to enforce the same authorization rules as the Kafka broker. 
It is further assumed that it will treat some topic names with suffixes which are not actually valid within Kafka as having access rules equivalent to the base topic name with no suffix. 
Currently this includes the following suffixes:

- :code:`+oversized`

Uploading
^^^^^^^^^

When uploading a message which is too large for a given target topic, the client should POST an HTTP(S) request to the URL formed by concatenating the URL provided by the :code:`LargeMessageUploadEndpoint` key in the broker's latest :code:`sys.metadata` message with the path :code:`/topic/${target_topic}+oversized`.
The body of the request must be a `BSON <https://bsonspec.org>`_ document conforming to the following schema:

.. code:: JSON

  {
    "type": "object",
    "properties": {
      "message": {
        "type": "string",
        "format": "binary",
        "description": "The body of the message."
      },
      "headers": {
        "type": "array",
        "description": "The Kafka headers attached to the message. 
                       This must be either an array of 2-tuples mapping 
                       strings to binary blobs, or an equivalent dictionary/object.",
        "items": {
          "type": "array",
          "minItems": 2,
          "maxItems": 2,
          "items": {
            "type": "string",
            "format": "binary",
          }
        }
      },
      "key": {
        "type": "string",
        "format": "binary",
        "description": "The Kafka key for the message."
      }
    },
    "required": ["message"],
  }

Where the JSONSchema notation of a string with format 'binary' should be understood to mean a BSON binary element. 

Message headers are technically optional, but in order to be able to refer to the message, the client should ensure that it includes an :code:`_id` header as described in :ref:`Standard Headers`. 

The offload API should respond to the POST request with HTTP status 201 'Created' if the message is successfully stored, or an appropriate error status code if the operation cannot be performed.

Placeholder/Reference Message
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Once the actual message payload has been successfully offloaded, the publishing client should send a placeholder or reference message over the Kafka topic in its place, so the subscribing clients learn of it. 
This is done by publishing an instance of the :code:`ExternalMessage` message model, which is currently just an envelope (rendered on the wire as a JSON object) containing the URL of the offloaded data, which should be constructed as: 

.. code::

  ${offload_endpoint}/msg/${message_ID}

where :code:`message_ID` is the UUID assigned to the message and sent as the :code:`_id` header value.

The placeholder message itself should be tagged by a a :code:`_format` header whose value is :code:`external` so that receiving clients can distinguish it as having specific semantics distinct from generic JSON messages. 

Downloading
^^^^^^^^^^^

When a client receives a placeholder message (identified by a :code:`_format` header with a value of :code:`external`), it may choose to automatically download (via an HTTP GET request) the referenced data and return that to the user, discarding the placeholder message itself. 

Since reading offloaded messages is subject to the same access controls as reading directly from Kafka, the client should be prepared to authenticate with the same credentials that it uses with Kafka.
However, to avoid possible security problems, the client should perform certain checks before doing so. 
Specifically, the credentials should only be sent/used when the external data URL matches the broker's declared offload API endpoint (obtained via the same `Discovery Mechanism`_). 
Additionally, if the URL specifies unencrypted HTTP as its scheme, the client may want to warn or refuse to use it. 
(Note that it is not completely unsafe to perform SCRAM authentication over an unencrypted connection, as the password is never transmitted in cleartext, but there can be man-in-the-middle issues, including the exchange being intercepted so that the operation performed is not the one requested by the user, or the payload data being leaked.)

If any necessary authentication (and associated access checks) succeeds, the offload API should send a response with an HTTP 200 'OK' status code and a body which contains the message data. 
The body should be a `BSON <https://bsonspec.org>`_ document with the same structure as described in :ref:`Uploading`, although additional top-level metadata elements may be included. 

Troubleshooting
---------------

Since large message support is an extension to the standard Kafka protocol, not all brokers will support it. 
This is mostly relevant when publishing, as a client subscribing to a topic on a broker without support should not encounter placeholder messages in the first place. 
When publishing, the client should generally detect when the extension is not supported and disable it automatically, either because it cannot read from the :code:`sys.metadata` topic, or because it receives a 'not authorized' response when attempting to query target topic configurations (to learn the per-topic message size limits). 

It has been observed, however, that some brokers, possibly due to misconfiguration, do not respond to topic configuration queries with either a success response or a definite denial, and instead cause the client to hang until the request timeout expires. 
Such a failure will produce an error similar to

.. code::

  hop: KafkaError{code=_TIMED_OUT,val=-185,str="Failed while waiting for response from broker: Local: Timed out"}

during Producer startup, before any messages are sent.
This both takes significant time, and cannot safely be interpreted as a definite lack of large message support because the same error can also occur transiently for other reasons, such as network connectivity problems, so it is not generally possible for the client to address this automatically by itself. 
In such cases, the user is advised to explicitly disable the offloading of large messages, as described :ref:`above <enable_disable>`. 
