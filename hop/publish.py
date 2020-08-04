from . import cli
from . import io


def _add_parser_args(parser):
    cli.add_client_opts(parser)
    parser.add_argument(
        "message", metavar="MESSAGE", nargs="+", help="One or more messages to publish.",
    )
    parser.add_argument(
        "-f",
        "--format",
        choices=io.Deserializer.__members__,
        default=io.Deserializer.BLOB.name,
        help="Specify the message format. Defaults to BLOB for an unstructured message.",
    )


def _main(args):
    """Parse and publish messages.

    """
    loader = io.Deserializer[args.format]
    stream = io.Stream(auth=(not args.no_auth))

    with stream.open(args.url, "w") as s:
        for message_file in args.message:
            message = loader.load_file(message_file)
            s.write(message)
