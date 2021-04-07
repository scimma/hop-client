================
Authentication
================

.. contents::
   :local:

Configuration
------------------

Since connections to the Hopskotch server require authentication, there
are several utilities exposed to generate and provide credentials for
both the CLI and python API. :code:`hop configure` provides command line
options to generate a configuration file with proper credentials needed
to authenticate.

In order to generate a configuration file, one can run :code:`hop configure setup`,
which prompts the user for a username and password to connect to Hopskotch
to publish or subscribe to messages. If you have the credentials csv file, you can
use it in the configuration file generation as 
:code:`hop configure setup --import <CREDENTIALS_FILE>` 

The default location for the configuration file can be found with :code:`hop configure locate`,
which points by default to :code:`${HOME}/.config/hop/config.toml`, but can be configured
by setting the :code:`XDG_CONFIG_PATH` variable.

Using Credentials
--------------------

Authentication is enabled by default and will read credentials from the
path resolved by :code:`hop configure locate`.

Multiple credentials may be stored together using this mechanism. 
Additional credentials may be added using `hop configure add-cred`, while the currently available
credentials may be displayed with `hop configure list-creds` and unwanted credentials can be removed
with `hop configure delete-cred`. Credentials can be added either interactively or from CSV files.
For removal credentials are specified by username, or username@hostname in case of ambiguity. 

When using the `hop` CLI to connect to connect to a kafka server, a credential will be selected
according to the following rules:

1. A credential with a matching hostname will be selected, unless no stored credential has a 
   matching hostname, in which case a credential with no specific hostname can be selected.
2. If a username is specified as part of the authority component of the URL (e.g. 
   `kafka://username@example.com/topic`) only credentials with that username will be considered.
3. If no username is specified and there is only one credential, which is not specifically 
   associated with any hostname, it will be used for all hosts. 

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
can set :code:`configure` to :code:`False`.
