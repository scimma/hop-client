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
            logger.info("Generating configuration with user-specified username + password")
            os.makedirs(os.path.dirname(config_file), exist_ok=True)

            user = input("Username: ")
            with open(config_file, "w") as f:
                toml.dump({"auth": {"username": user, "password": getpass.getpass()}}, f)
            logger.info(f"Generated configuration at: {config_file}")
    elif args.command is None:
        logger.warning("Please use any of these commands: locate or setup")
