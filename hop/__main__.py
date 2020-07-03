import argparse
import signal

from . import __version__
from . import auth
from . import publish
from . import subscribe
from . import version


def append_subparser(subparser, cmd, func):
    assert func.__doc__, "empty docstring: {}".format(func)
    help_ = func.__doc__.split("\n")[0].lower().strip(".")
    desc = func.__doc__.strip()

    parser = subparser.add_parser(
        cmd,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        help=help_,
        description=desc,
    )

    parser.set_defaults(func=func)
    return parser


def set_up_cli():
    """Set up CLI commands for hop entry point.

    Returns:
        An ArgumentParser instance with hop-based commands and relevant
        arguments added for all commands.

    """
    parser = argparse.ArgumentParser(prog="hop")
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s version {__version__}",
    )

    # set up subparser
    subparser = parser.add_subparsers(title="Commands", metavar="<command>", dest="cmd")
    subparser.required = True

    # register commands
    p = append_subparser(subparser, "auth", auth._main)
    auth._add_parser_args(p)

    p = append_subparser(subparser, "publish", publish._main)
    publish._add_parser_args(p)

    p = append_subparser(subparser, "subscribe", subscribe._main)
    subscribe._add_parser_args(p)

    p = append_subparser(subparser, "version", version._main)

    return parser


def main():
    signal.signal(signal.SIGINT, signal.SIG_DFL)

    parser = set_up_cli()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
