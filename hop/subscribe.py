#!/usr/bin/env python

__author__ = "Bryce Cousins (bfc5288@psu.edu)"
__description__ = "tools to receive and parse messages"


import json

from . import cli
from . import io


def print_message(message_model, json_dump=False):
    """Print the content of a message.

    Args:
      message_model: dataclass model object for a message
      json_dump: boolean indicating whether to print as raw json

    Returns:
      None
    """

    # print the message content
    if json_dump:
        print(json.dumps(message_model.asdict()))
    else:
        print(str(message_model))


# ------------------------------------------------
# -- main


def _add_parser_args(parser):
    cli.add_url_opts(parser)

    # consumer options
    parser.add_argument(
        "-s",
        "--start-at",
        choices=io.StartPosition.__members__,
        default=str(io.StartPosition.LATEST),
        help="Set the message offset offset to start at. Default: LATEST.",
    )
    parser.add_argument(
        "-p",
        "--persist",
        action="store_true",
        help="If set, persist or listen to messages indefinitely. "
             "Otherwise, will stop listening when EOS is received.",
    )
    parser.add_argument(
        "-j", "--json", help="Request message output as raw json", action="store_true",
    )


def _main(args):
    """Receive and parse messages.

    """
    stream = io.Stream(start_at=args.start_at, persist=args.persist)

    with stream.open(args.url, "r") as s:
        for message in s:
            print_message(message, args.json)
