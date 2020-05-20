#!/usr/bin/env python

__author__ = "Bryce Cousins (bfc5288@psu.edu)"
__description__ = "tools to receive and parse GCN circulars"


import argparse
import json

from . import cli
from .io import Stream
from .models import GCNCircular, VOEvent


def classify_msg(msg):
    """Check and classify the format of a message obtained from an ADC
    stream, use it to instantiate a data model corresponding to that format.

    Args:
      msg:       raw message from an ADC stream

    Returns:
      msg_model: dataclass model object for the raw message

    """
    # check for msg format using standard-specific flags
    voevent_flag = "ivorn"
    gcncir_flag = "GCN CIRCULAR"

    # VOEvent:
    if voevent_flag in msg:
        msg_model = VOEvent(**msg)
        status_str = "## Parsing a VOEvent"
    # GCN circular:
    elif gcncir_flag in msg:
        msg_model = GCNCircular(**msg)
        status_str = "## Parsing a GCN Circular"
    else:
        msg_model = GCNCircular(**msg)
        status_str = "## Parsing a hop-published message"

    print(status_str)

    return msg_model


def print_gcn(msg_model, json_dump=False):
    """Print the content of a gcn message.

    Args:
      msg_model:       dataclass model object for a message
      json_dump: boolean indicating whether to print as raw json

    Returns:
      None
    """

    if json_dump:
        print(json.dumps(msg_model))
    else:
        print(str(msg_model))


# ------------------------------------------------
# -- main


def _add_parser_args(parser):
    cli.add_url_opts(parser)
    cli.add_config_opts(parser)

    # consumer options
    parser.add_argument(
        "-j", "--json", help="Request gcn output as raw json", action="store_true",
    )
    parser.add_argument(
        "-e",
        "--earliest",
        help="Request to stream from the earliest available Kafka offset",
        action="store_true",
    )
    parser.add_argument(
        "-t",
        "--timeout",
        type=float,
        default=10,
        help="Specifies the time (in seconds) to wait for messages before timing out; "
        "specify -1 to wait indefinitely.  Default: 10 seconds",
    )


def _main(args=None):
    """Receive and parse GCN circulars.

    """
    if not args:
        parser = argparse.ArgumentParser()
        _add_parser_args(parser)
        args = parser.parse_args()

    # load config if specified
    config = cli.load_config(args)

    # set offset
    start_offset = "earliest" if args.earliest else "latest"

    # set timeout
    timeout = None if args.timeout == -1 else args.timeout

    # read from topic

    # assume json format for the gcn
    gcn_format = "json"

    stream = Stream(format=gcn_format, config=config, start_at=start_offset)
    with stream.open(args.url, "r") as s:
        for msg in s(timeout=timeout):
            msg_model = classify_msg(msg)
            print_gcn(msg_model, args.json)
