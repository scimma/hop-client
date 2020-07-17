import argparse
import errno
import getpass
import logging
import os

import toml

from adc import auth

logger = logging.getLogger("hop")

SASLMethod = auth.SASLMethod


# thin wrapper over adc's SASLAuth to define
# different defaults used within Hopskotch
class Auth(auth.SASLAuth):
    """Attach SASL-based authentication to a client.

    Returns client-based auth options when called.

    Parameters
    ----------
    user : `str`
        Username to authenticate with.
    password : `str`
        Password to authenticate with.
    ssl : `bool`, optional
        Whether to enable SSL (enabled by default).
    method : `SASLMethod`, optional
        The SASL method to authenticate, default = SASLMethod.SCRAM_SHA_512.
        See valid SASL methods in SASLMethod.
    ssl_ca_location : `str`, optional
        If using SSL via a self-signed cert, a path/location
        to the certificate.
    """

    def __init__(self, user, password, ssl=True, method=SASLMethod.SCRAM_SHA_512, **kwargs):
        super().__init__(user, password, ssl=ssl, method=method, **kwargs)


def get_auth_path():
    """Determines the default location for auth configuration.

    Returns:
        The path to the authentication configuration file.

    """
    auth_filepath = os.path.join("hop", "config.toml")
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
        config = toml.loads(f.read())["auth"]

    # translate config options
    ssl = True
    extra_kwargs = {}
    try:
        # SSL options
        if "protocol" in config and config["protocol"] == "SASL_PLAINTEXT":
            ssl = False
        elif "ssl_ca_location" in config:
            extra_kwargs["ssl_ca_location"] = config["ssl_ca_location"]

        # SASL options
        user = config["username"]
        password = config["password"]

        if "mechanism" in config:
            mechanism = config["mechanism"].replace("-", "_")
        else:
            mechanism = "SCRAM_SHA_512"

    except KeyError:
        raise KeyError("configuration file is not configured correctly")
    else:
        return Auth(user, password, ssl=ssl, method=SASLMethod[mechanism], **extra_kwargs)


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
            logger.info("generating configuration with user-specified username + password")
            os.makedirs(os.path.dirname(authfile), exist_ok=True)

            user = input("Username: ")
            with open(authfile, "w") as f:
                toml.dump({"auth": {"username": user, "password": getpass.getpass()}}, f)
            logger.info(f"generated configuration at: {authfile}")
