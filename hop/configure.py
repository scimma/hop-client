import argparse
import getpass
import logging
import os
import csv
import errno

import toml

logger = logging.getLogger("hop")


def get_config_path():
    """Determines the default location for auth configuration.

    Returns:
        The path to the authentication configuration file.

    """
    auth_filepath = os.path.join("hop", "config.toml")
    if "XDG_CONFIG_HOME" in os.environ:
        return os.path.join(os.getenv("XDG_CONFIG_HOME"), auth_filepath)
    else:
        return os.path.join(os.getenv("HOME"), ".config", auth_filepath)


def _add_parser_args(parser):
    subparser = parser.add_subparsers(title="Commands", metavar="<command>", dest="command")
    subparser.add_parser(
        "locate",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        help="display configuration path",
    )

    setup_subparser = subparser.add_parser(
        "setup",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        help="set up configuration",
    )
    setup_subparser.add_argument(
        "-f", "--force", action="store_true", help="If set, overrides current configuration",
    )

    setup_subparser.add_argument(
        "-i", "--import", dest="import_cred", help="Import credentilas from CSV file",
    )


def write_config_file(config_file, username, password):
    """
        Write configuration file for the given username and password

        Args:
            config_file: configuration file path
            username: username at hopskotch
            password: password at hopskotch
    """

    os.makedirs(os.path.dirname(config_file), exist_ok=True)
    with open(config_file, "w") as f:
        toml.dump({"auth": {"username": username, "password": password}}, f)
        logger.info(f"Generated configuration at: {config_file}")


def set_up_configuration(config_file, csv_file):
    """
        Setup configuration file

        Args:
            config_file: Configuration file path
            csv_file: Path to csv credentials file
    """

    if csv_file is None:
        logger.info("Generating configuration with user-specified username + password")

        username = input("Username: ")
        write_config_file(config_file, username, getpass.getpass())

    else:
        if os.path.exists(csv_file):
            with open(csv_file, "r") as f:
                reader = csv.DictReader(f)
                creds = next(reader)
                write_config_file(config_file, creds["username"], creds["password"])
        else:
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), csv_file)


def _main(args):
    """Configuration utilities.

    """
    config_file = get_config_path()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s | %(name)s : %(levelname)s : %(message)s",
    )

    if args.command == "locate":
        print(config_file)
    elif args.command == "setup":
        if os.path.exists(config_file) and not args.force:
            logger.warning("Configuration already exists, overwrite file with --force")
        else:
            set_up_configuration(config_file, args.import_cred)
    elif args.command is None:
        logger.warning("Please use any of these commands: locate or setup")
