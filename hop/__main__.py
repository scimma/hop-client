import argparse
import logging
import sys
import traceback

from . import __version__
from . import configure
from . import auth
from . import publish
from . import subscribe
from . import version
from . import list_topics
from .utils import cli as cli_utils


logger = logging.getLogger("hop")


def set_up_cli():
    """Set up CLI commands for hop entry point.

    Returns:
        An ArgumentParser instance with hop-based commands and relevant
        arguments added for all commands.

    """
    parser = argparse.ArgumentParser(prog="hop", formatter_class=cli_utils.SubcommandHelpFormatter)
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s version {__version__}",
    )

    parser.add_argument(
        "-d", "--debug", action="store_true",
        help="Display more verbose errors with internal details",
    )

    # set up subparser
    subparser = parser.add_subparsers(title="commands", metavar="<command>", dest="cmd")
    subparser.required = True

    # register commands
    p = cli_utils.append_subparser(
        subparser,
        "configure",
        configure._main,
        formatter_class=cli_utils.SubcommandHelpFormatter
    )
    configure._add_parser_args(p)

    p = cli_utils.append_subparser(subparser, "auth", auth._main)
    auth._add_parser_args(p)

    p = cli_utils.append_subparser(subparser, "publish", publish._main)
    publish._add_parser_args(p)

    p = cli_utils.append_subparser(subparser, "subscribe", subscribe._main)
    subscribe._add_parser_args(p)

    p = cli_utils.append_subparser(subparser, "version", version._main)

    p = cli_utils.append_subparser(subparser, "list-topics", list_topics._main)
    list_topics._add_parser_args(p)

    return parser


def check_auth_data(prog_name):
    """Advise the user on getting/configuring credential data
    """

    advice = """
No valid credential data found
You can get a credential from https://my.hop.scimma.org
To load your credential, run `hop auth add`
"""

    try:
        auth.load_auth()
    except FileNotFoundError:
        print(advice)
    except Exception as ex:
        print(prog_name + ":", ex, file=sys.stderr)
        print(advice)


def main():
    parser = set_up_cli()

    try:
        args = parser.parse_args()
    except SystemExit:
        check_auth_data(parser.prog)
        raise
    try:
        args.func(args)
    except KeyboardInterrupt:
        logger.info("received keyboard interrupt, closing")
    except Exception as ex:
        if args.debug:
            traceback.print_exc(file=sys.stderr)
        else:
            print(parser.prog + ":", ex, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
