import logging
import sys

from . import cli
from . import io


logger = logging.getLogger("hop")


def _add_parser_args(parser):
    cli.add_client_opts(parser)
    cli.add_logging_opts(parser)

    parser.add_argument(
        "message", metavar="MESSAGE", nargs="*",
        help="File of messages to publish. (standard input is used if omitted)"
    )
    parser.add_argument(
        "-f",
        "--format",
        choices=io.Deserializer.__members__,
        default=io.Deserializer.BLOB.name,
        help="Specify the message format. Defaults to BLOB for an unstructured message.",
    )
    parser.add_argument(
        "-t", "--test",
        action="store_true",
        help="Mark messages as test messages by adding a header with key '_test'."
    )


def _main(args):
    """Publish messages.

    """
    cli.set_up_logger(args)

    loader = io.Deserializer[args.format]
    stream = io.Stream(auth=(not args.no_auth))

    with stream.open(args.url, "w") as s:
        logger.info("publishing messages to stream")
        for message_file in args.message:
            message = loader.load_file(message_file)
            s.write(message)

        if len(args.message) == 0:
            messages = sys.stdin.read().splitlines()

            if messages:
                assert args.format == io.Deserializer.BLOB.name \
                    or args.format == io.Deserializer.JSON.name, \
                    "piping/redirection only allowed for BLOB and JSON formats"

                for message in messages:
                    s.write(loader.load(message), test=args.test)
