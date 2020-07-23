================
Authentication
================

.. contents::
   :local:

Configuration
------------------

Since connections to the Hopskotch server require authentication, there
are several utilities exposed to generate and provide credentials for
both the CLI and python API. :code:`hop auth` provides command line
options to generate a configuration file with proper credentials needed
to authenticate.

In order to generate a configuration file, one can run :code:`hop auth setup`,
which prompts the user for a username and password to connect to Hopskotch
to publish or subscribe to messages.

The default location for the configuration file can be found with :code:`hop auth locate`,
which points by default to :code:`${HOME}/.config/hop/config.toml`, but can be configured
by setting the :code:`XDG_CONFIG_PATH` variable.

Using Credentials
--------------------

Authentication is enabled by default and will read credentials from the
path resolved by :code:`hop auth locate`.

For the python API, one can modify various authentication options by passing
in an :code:`Auth` instance with credentials to a :code:`Stream` instance.
This provides a similar interface to authenticating as with the requests library.

.. code:: python

    from hop import Stream
    from hop.auth import Auth

    auth = Auth("my-username", "my-password")
    stream = Stream(auth=auth)

    with stream.open("kafka://hostname:port/topic", "w") as s:
        s.write({"my": "message"})

In order to disable authentication in the command line interface, you can
pass :code:`--no-auth` for various CLI commands. For the python API, you
can set :code:`auth` to :code:`False`.
