#!/usr/bin/env python

__author__ = "Patrick Godwin (patrick.godwin@psu.edu)"
__description__ = "tools to parse and publish messages"


from .auth import load_auth
from . import cli
from . import io


# ------------------------------------------------
# -- main


def _add_parser_args(parser):
    cli.add_client_opts(parser)
    parser.add_argument(
        "message", metavar="MESSAGE", nargs="+", help="One or more messages to publish.",
    )
    parser.add_argument(
        "-f",
        "--format",
        choices=io.Deserializer.__members__,
        default=str(io.Deserializer.BLOB),
        help="Specify the message format. Defaults to BLOB for an unstructured message.",
    )


def _main(args):
    """Parse and publish messages.

    """
    auth = load_auth() if not args.no_auth else None
    loader = io.Deserializer[args.format]
    stream = io.Stream(auth=auth)

    with stream.open(args.url, "w") as s:
        for message_file in args.message:
            message = loader.load_file(message_file)
            s.write(message)
