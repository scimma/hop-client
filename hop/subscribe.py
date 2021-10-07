import json
import logging
import sys

from . import cli
from . import io
from . import models


logger = logging.getLogger("hop")


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
    print(message, file=sys.stdout, flush=True)


def _add_parser_args(parser):
    cli.add_client_opts(parser)
    cli.add_logging_opts(parser)

    # consumer options
    parser.add_argument(
        "-s",
        "--start-at",
        choices=io.StartPosition.__members__,
        default=str(io.StartPosition.LATEST).upper(),
        help="Set the message offset offset to start at. Default: LATEST.",
    )
    parser.add_argument(
        "-e",
        "--until-eos",
        action="store_true",
        help="If set, only subscribe until EOS is received (end of stream). "
             "Otherwise, listen to messages indefinitely.",
    )
    parser.add_argument(
        "-g", "--group-id",
        default=None,
        help="Consumer group ID. If unset, a random ID will be generated."
    )
    parser.add_argument(
        "-j", "--json", help="Request message output as raw json", action="store_true",
    )


def _main(args):
    """Subscribe to messages.

    """
    cli.set_up_logger(args)

    start_at = io.StartPosition[args.start_at]
    stream = io.Stream(auth=(not args.no_auth), start_at=start_at, until_eos=args.until_eos)

    with stream.open(args.url, "r", group_id=args.group_id) as s:
        for message in s:
            print_message(message, args.json)
