import argparse
import signal

from . import __version__
from . import configure
from . import publish
from . import subscribe
from . import version
from .utils import cli as cli_utils


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

    p = cli_utils.append_subparser(subparser, "publish", publish._main)
    publish._add_parser_args(p)

    p = cli_utils.append_subparser(subparser, "subscribe", subscribe._main)
    subscribe._add_parser_args(p)

    p = cli_utils.append_subparser(subparser, "version", version._main)

    return parser


def main():
    signal.signal(signal.SIGINT, signal.SIG_DFL)

    parser = set_up_cli()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
