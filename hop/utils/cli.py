import argparse


def append_subparser(subparser, cmd, func, formatter_class=None):
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

    if formatter_class:
        parser = subparser.add_parser(
            cmd,
            help=help_,
            description=desc,
            formatter_class=formatter_class
        )
    else:
        parser = subparser.add_parser(cmd, help=help_, description=desc)

    parser.set_defaults(func=func)

    return parser


class SubcommandHelpFormatter(argparse.RawDescriptionHelpFormatter):
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
