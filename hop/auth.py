import argparse
import errno
import logging
import os
import webbrowser


from adc import auth

DEFAULT_AUTH_CONFIG = """\
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.username=<USERNAME>
sasl.password=<PASSWORD>
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


def write_config_file(config_file, force, credentials_file):

    if os.path.exists(config_file) and not force:
        logger.warning("Configuration already exists, overwrite file with --force")
    else:
        username = "username"
        password = "password"
        if credentials_file is not None:
            if not os.path.exists(credentials_file):
                logger.warning("Credentials file does not exist")
            else:
                with open(credentials_file) as cf:
                    header = cf.readline().split(",")
                    values = cf.readline().split(",")
                    # extract the username and password according to the arrangment in header
                    # this to avoid extracting wrong data from cred file if we added new fields
                    # before the username and password and/or if the user has an old version of the
                    # credentials file while we updated it with new fields.
                    i = 0
                    for item in header:
                        item = item.rstrip()
                        if item == "username":
                            username = values[i].rstrip()
                        elif item == "password":
                            password = values[i].rstrip()
                        i += 1

        os.makedirs(os.path.dirname(config_file), exist_ok=True)
        configuration = DEFAULT_AUTH_CONFIG.replace("<USERNAME>", username)
        configuration = configuration.replace("<PASSWORD>", password)
        with open(config_file, "w") as f:
            f.write(configuration)
        logger.info("Your configuration file has been written at " + config_file)


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

    setup_subparser.add_argument(
        "-c", "--credentials_file", help="uses these credentials file to write the configuration file",
    )

    subparser.add_parser(
        "web",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        help="Open hopskotch authentication website",
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
        write_config_file(authfile, args.force, args.credentials_file)
    elif args.command == "web":
        webbrowser.open('https://admin.dev.hop.scimma.org/hopauth/', new=1)
