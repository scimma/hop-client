import argparse
import signal

from . import __version__
from . import configure
from . import publish
from . import subscribe
from . import version


def append_subparser(subparser, cmd, func, formatter=False):
    """Attach a command to a subparser, generating help from docstrings.

    Args:
        subparser: An argparse-based subparser to attach a command to.
        cmd: The command name.
        func: The function to call for said command.
        formatter: Whether to apply custom formatting for sub-subparser commands.

    """
    assert func.__doc__, "empty docstring: {}".format(func)
    help_ = func.__doc__.split("\n")[0].lower().strip(".")
    desc = func.__doc__.strip()

    if formatter:
        parser = subparser.add_parser(
            cmd,
            help=help_,
            description=desc,
            formatter_class=_SubcommandHelpFormatter
        )
    else:
        parser = subparser.add_parser(cmd, help=help_, description=desc)

    parser.set_defaults(func=func)

    return parser


class _SubcommandHelpFormatter(argparse.RawDescriptionHelpFormatter):
    """Custom formatter for displaying subcommands.

    """
    def _format_action(self, action):
        # fixes spacing/indent introduced by metavar in subparser
        self._action_max_length = 20
        parts = super(argparse.RawDescriptionHelpFormatter, self)._format_action(action)

        # removes the metavar description between title and commands
        if action.nargs == argparse.PARSER:
            parts = "\n".join(parts.split("\n")[1:])

        return parts


def set_up_cli():
    """Set up CLI commands for hop entry point.

    Returns:
        An ArgumentParser instance with hop-based commands and relevant
        arguments added for all commands.

    """
    parser = argparse.ArgumentParser(prog="hop", formatter_class=_SubcommandHelpFormatter)
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s version {__version__}",
    )

    # set up subparser
    subparser = parser.add_subparsers(title="commands", metavar="<command>", dest="cmd")
    subparser.required = True

    # register commands
    p = append_subparser(subparser, "configure", configure._main, formatter=True)
    configure._add_parser_args(p)

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
