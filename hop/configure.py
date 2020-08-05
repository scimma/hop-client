import argparse
import getpass
import logging
import os

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
        "-i", "--import_cred", help="Import credentilas from CSV file",
    )


def write_config_file(config_file, username, password):

    os.makedirs(os.path.dirname(config_file), exist_ok=True)
    with open(config_file, "w") as f:
        toml.dump({"auth": {"username": username, "password": password}}, f)
        logger.info(f"Generated configuration at: {config_file}")


def configuration_setup(config_file, is_force, csv_file):

    if os.path.exists(config_file) and not is_force:
        logger.warning("Configuration already exists, overwrite file with --force")

    elif csv_file is None:
        logger.info("Generating configuration with user-specified username + password")

        username = input("Username: ")
        write_config_file(config_file, username, getpass.getpass())

    else:
        try:
            cf = open(csv_file)
        except OSError:
            logger.warning(f"File {csv_file} cannot be opened")
        else:
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
            write_config_file(config_file, username, password)


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
        print(args)
        configuration_setup(config_file, args.force, args.import_cred)
    elif args.command is None:
        logger.warning("Please use any of these commands: locate or setup")
