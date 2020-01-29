#!/usr/bin/env python

__author__ = "Patrick Godwin (patrick.godwin@psu.edu)"

import argparse
import logging
import signal

from . import __version__


# ------------------------------------------------
# -- CLI utilities


def append_subparser(subparser, cmd, func):
    assert func.__doc__, "empty docstring: {}".format(func)
    help_ = func.__doc__.split("\n")[0].lower().strip(".")
    desc = func.__doc__.strip()

    parser = subparser.add_parser(
        cmd,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        help=help_,
        description=desc,
    )

    parser.set_defaults(func=func)
    return parser


def set_up_cli():
    """Set up CLI commands for scimma entry point.

    """
    parser = argparse.ArgumentParser(prog="scimma")
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s-client version {__version__}",
    )

    subparser = parser.add_subparsers(title="Commands", metavar="<command>", dest="cmd")
    subparser.required = True

    return parser


# ------------------------------------------------
# -- main


def main():
    signal.signal(signal.SIGINT, signal.SIG_DFL)

    parser = set_up_cli()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
