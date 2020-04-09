#!/usr/bin/env python

__author__ = "Patrick Godwin (patrick.godwin@psu.edu)"
__description__ = "a module for CLI utilities"


def add_url_opts(parser):
    parser.add_argument(
        "url",
        metavar="URL",
        help="Sets the URL (kafka://host[:port]/topic) to publish GCNs to.",
    )


def add_config_opts(parser):
    config = parser.add_mutually_exclusive_group()
    config.add_argument(
        "-F", "--config-file", help="Set client configuration from file.",
    )
    config.add_argument(
        "-X",
        "--config",
        action="append",
        help="Set client configuration via prop=val. Can be specified multiple times.",
    )


def load_config(args):
    if args.config_file:
        return args.config_file
    elif args.config:
        return {opt[0]: opt[1] for opt in (kv.split("=") for kv in args.config)}
    else:
        return None
