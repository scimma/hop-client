==========
Commands
==========

.. contents::
   :local:


**hop-client** provides a command line interface for various tasks:

:code:`hop publish`: parse and publish GCN circulars and notices
:code:`hop subscribe`: parse and print GCN circulars and notices

:code:`hop publish`
~~~~~~~~~~~~~~~~~~~~~~

This command allows a user to parse an `RFC 822 formatted GCN circular <https://gcn.gsfc.nasa.gov/gcn3_circulars.html>`_
or an XML formatted `GCN/VOEvent notice <https://gcn.gsfc.nasa.gov/tech_describe.html>`_
and publish it as a JSON-formatted GCN via Kafka.

.. program-output:: hop publish --help
   :nostderr:


:code:`hop subscribe`
~~~~~~~~~~~~~~~~~~~~~~

This command allows a user to parse a JSON-formatted GCN from a Kafka
topic and display it on stdout via a pretty-print or JSON dump.

.. program-output:: hop subscribe --help
   :nostderr:
