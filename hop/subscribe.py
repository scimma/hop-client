import json

from . import cli
from . import io
from . import models


def print_message(message, json_dump=False):
    """Print the content of a message.

    Args:
      message: message to print
      json_dump: boolean indicating whether to print as raw json

    Returns:
      None
    """
    if json_dump:
        if isinstance(message, models.MessageModel):
            message = json.dumps(message.asdict())
        else:
            message = json.dumps(message)
    print(message)


def _add_parser_args(parser):
    cli.add_client_opts(parser)

    # consumer options
    parser.add_argument(
        "-s",
        "--start-at",
        choices=io.StartPosition.__members__,
        default=str(io.StartPosition.LATEST).upper(),
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
    """Subscribe to messages.

    """
    start_at = io.StartPosition[args.start_at]
    stream = io.Stream(auth=(not args.no_auth), start_at=start_at, persist=args.persist)

    with stream.open(args.url, "r") as s:
        for message in s:
            print_message(message, args.json)
