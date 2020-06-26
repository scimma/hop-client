==========
Commands
==========

.. contents::
   :local:


**hop-client** provides a command line interface for various tasks:

:code:`hop publish`: parse and publish messages such as GCN circulars and notices
:code:`hop subscribe`: parse and print messages such as GCN circulars and notices

:code:`hop publish`
~~~~~~~~~~~~~~~~~~~~~~

This command allows a user to parse an `RFC 822 formatted GCN circular <https://gcn.gsfc.nasa.gov/gcn3_circulars.html>`_, an XML formatted `GCN/VOEvent notice <https://gcn.gsfc.nasa.gov/tech_describe.html>`_, or an unstructured string or text file and publish it as via Kafka. Structured messages
such as GCN circulars and VOEvents are published as JSON-formatted text.

.. program-output:: hop publish --help
   :nostderr:


:code:`hop subscribe`
~~~~~~~~~~~~~~~~~~~~~~

This command allows a user to parse a message from a Kafka topic and display it on stdout via
a pretty-print or JSON dump.

.. program-output:: hop subscribe --help
   :nostderr:

:code:`hop version`
~~~~~~~~~~~~~~~~~~~~~~

This command prints all the versions of the dependencies

.. program-output:: hop version --help
   :nostderr: