#!/usr/bin/env python

__author__ = "Patrick Godwin (patrick.godwin@psu.edu)"
__description__ = "a module for auth utilities"

import argparse
import errno
import logging
import os

from adc import auth

DEFAULT_AUTH_CONFIG = """\
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.username=username
sasl.password=password
"""

logger = logging.getLogger("hop")

# expose auth options from adc
SASLAuth = auth.SASLAuth
SASLMethod = auth.SASLMethod

# map default auth option to Auth
Auth = auth.SASLAuth


def get_auth_path():
    """Determines the default location for auth configuration.

    Returns:
        The path to the authentication configuration file.

    """
    auth_filepath = os.path.join("hop", "auth.conf")
    if "XDG_CONFIG_HOME" in os.environ:
        return os.path.join(os.getenv("XDG_CONFIG_HOME"), auth_filepath)
    else:
        return os.path.join(os.getenv("HOME"), ".config", auth_filepath)


def load_auth(authfile=get_auth_path()):
    """Configures an Auth instance given a configuration file.

    Args:
        authfile: Path to a configuration file, loading from
            the default location if not given.

    Returns:
        A configured Auth instance.

    Raises:
        KeyError: An error occurred parsing the configuration file.

    """
    if not os.path.exists(authfile):
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), authfile)

    # load config
    with open(authfile, "r") as f:
        config = {opt[0]: opt[1] for opt in (line.split("=") for line in f.read().splitlines())}

    # translate config options
    ssl = True
    extra_kwargs = {}
    try:
        # SSL options
        if config["security.protocol"] == "SASL_PLAINTEXT":
            ssl = False
        elif "ssl.ca.location" in config:
            extra_kwargs["ssl_ca_location"] = config["ssl.ca.location"]

        # SASL options
        user = config["sasl.username"]
        password = config["sasl.password"]
        method = SASLMethod[config["sasl.mechanism"].replace("-", "_")]
    except KeyError:
        raise KeyError("configuration file is not configured correctly")
    else:
        return Auth(user, password, ssl=ssl, method=method, **extra_kwargs)


def _add_parser_args(parser):
    subparser = parser.add_subparsers(title="Commands", metavar="<command>", dest="command")
    subparser.add_parser(
        "locate",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        help="display authentication config path",
    )

    setup_subparser = subparser.add_parser(
        "setup",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        help="set up authentication config with defaults",
    )
    setup_subparser.add_argument(
        "-f", "--force", action="store_true", help="If set, overrides current configuration",
    )


def _main(args):
    """Authentication utilities.

    """
    authfile = get_auth_path()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s | %(name)s : %(levelname)s : %(message)s",
    )

    if args.command == "locate":
        print(authfile)
    elif args.command == "setup":
        if os.path.exists(authfile) and not args.force:
            logger.warning("configuration already exists, overwrite file with --force")
        else:
            os.makedirs(os.path.dirname(authfile), exist_ok=True)
            with open(authfile, "w") as f:
                f.write(DEFAULT_AUTH_CONFIG)
