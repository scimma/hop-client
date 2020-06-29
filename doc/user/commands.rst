==========
Commands
==========

.. contents::
   :local:


**hop-client** provides a command line interface for various tasks:

:code:`hop auth`: Authentication utilities
:code:`hop publish`: Publish messages such as GCN circulars and notices
:code:`hop subscribe`: Listen to messages such as GCN circulars and notices
:code:`hop version`: Print version dependencies of :code:`hop-client`

:code:`hop auth`
~~~~~~~~~~~~~~~~~~~~~~

This command allows a user to handle auth-based configuration.

.. program-output:: hop auth --help
   :nostderr:

:code:`hop publish`
~~~~~~~~~~~~~~~~~~~~~~

This command allows a user to parse various structured and unstructured messages, including:

* `RFC 822 formatted GCN circular <https://gcn.gsfc.nasa.gov/gcn3_circulars.html>`_
* An XML formatted `GCN/VOEvent notice <https://gcn.gsfc.nasa.gov/tech_describe.html>`_
* An unstructured string or text file and publish it as via Kafka.

Structured messages such as GCN circulars and VOEvents are published as JSON-formatted text.

.. program-output:: hop publish --help
   :nostderr:


:code:`hop subscribe`
~~~~~~~~~~~~~~~~~~~~~~

This command allows a user to subscribe to messages and print them to stdout.

.. program-output:: hop subscribe --help
   :nostderr:

:code:`hop version`
~~~~~~~~~~~~~~~~~~~~~~

This command prints all the versions of the dependencies

.. program-output:: hop version --help
   :nostderr:
