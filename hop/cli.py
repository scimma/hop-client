import logging
import sys


logger = logging.getLogger("hop")


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


def add_logging_opts(parser):
    """Add logging client options to an argument parser.

    Args:
        parser: An ArgumentParser instance to add client options to.

    """
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "-q", "--quiet", action="store_true", help="If set, only display warnings and errors."
    )
    group.add_argument(
        "-v", "--verbose", action="store_true", help="If set, display additional logging messages."
    )


def get_log_level(args):
    """Determine the log level from logging options.

    Args:
        args: The parsed argparse arguments.

    Returns:
        the logging log level.

    """
    if args.quiet:
        return logging.WARNING
    elif args.verbose:
        return logging.DEBUG
    else:
        return logging.INFO


def set_up_logger(args):
    """Set up common logging settings for CLI usage.

    Args:
        args: The parsed argparse arguments.

    """
    log_level = get_log_level(args)
    logger.setLevel(log_level)
    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(log_level)
    formatter = logging.Formatter("%(asctime)s | hop : %(levelname)s : %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
