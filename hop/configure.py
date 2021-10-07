import logging
import os

from . import cli


logger = logging.getLogger("hop")


def get_config_path(type: str = "general"):
    """Determines the default location for auth configuration.

    Args:
        type: The type of configuration data for which the path should be looked up.
              Recognized types are 'general' and 'auth'.

    Returns:
        The path to the requested configuration file.

    Raises:
        ValueError: Unrecognized config type requested.

    """

    file_for_type = {
        "general": "config.toml",
        "auth": "auth.toml",
    }

    if type not in file_for_type:
        raise ValueError(f"Unknown configuration type: '{type}'")

    auth_filepath = os.path.join("hop", file_for_type[type])
    if "XDG_CONFIG_HOME" in os.environ:
        return os.path.join(os.getenv("XDG_CONFIG_HOME"), auth_filepath)
    else:
        return os.path.join(os.getenv("HOME"), ".config", auth_filepath)


def _add_parser_args(parser):
    cli.add_logging_opts(parser)

    subparser = parser.add_subparsers(title="commands", metavar="<command>", dest="command")
    subparser.required = True

    locate_parser = subparser.add_parser("locate", help="display configuration path")
    locate_parser.add_argument("-t", "--type", dest="type", default="general", required=False,
                               choices=["general", "auth"],
                               help="The type of configuration file to locate. "
                               "Defaults to general configuration.")


def _main(args):
    """Configuration utilities.

    """
    cli.set_up_logger(args)

    if args.command == "locate":
        print(get_config_path(args.type))
