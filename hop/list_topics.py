from . import cli
from .io import list_topics


def _main(args):
    """List available topics.

    """
    valid_topics = list_topics(args.url, auth=not args.no_auth)
    if len(valid_topics) == 0:
        print("No accessible topics")
    else:
        print("Accessible topics:")
        for topic in sorted(valid_topics.keys()):
            print(f" {topic}")


def _add_parser_args(parser):
    cli.add_client_opts(parser)
