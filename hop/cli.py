def add_client_opts(parser):
    """Add general client options to an argument parser.

    Args:
        parser: An ArgumentParser instance to add client options to.

    """

    parser.add_argument(
        "url",
        metavar="URL",
        help="Sets the URL (kafka://host[:port]/topic) to publish messages to.",
    )
    parser.add_argument(
        "--no-auth", action="store_true", help="If set, disable authentication."
    )
