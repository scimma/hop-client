#!/usr/bin/env python

__author__ = "Bryce Cousins (bfc5288@psu.edu)"
__description__ = "tools to receive and parse GCN circulars"


import argparse
import json
import warnings

from . import cli
from .io import Stream
from .models import GCNCircular, VOEvent


def print_msg(msg, json_dump=False):
    """Check the format of a message obtained from an ADC stream, use it to instantiate
    a data model corresponding to that format, and print the message.

    Args:
      msg:       the raw message from an ADC stream
      json_dump: boolean indicating whether to print msg as raw json

    Returns:
      None
    """

    # check for msg format using standard-specific flags
    voevent_flag = 'ivorn'
    gcncir_flag = 'GCN CIRCULAR'

    # VOEvent:
    if voevent_flag in msg:
        gcn = VOEvent(**msg)
        status_str = ("##################################################\n"
                      "######## Hop-client: parsing a VOEvent  ##########\n"
                      "##################################################")
    # GCN circular:
    elif gcncir_flag in msg:
        gcn = GCNCircular(**msg)
        status_str = ("##################################################\n"
                      "####### Hop-client: parsing a GCN Circular #######\n"
                      "##################################################")
    else:
        try:
            gcn = GCNCircular(**msg)
            status_str = ("##################################################\n"
                          "## Hop-client: parsing a hop-published message ###\n"
                          "##################################################")
        except:
            warnings.warn('#### Warning: message format not recognized; dumping as json ####')
            gcn = msg
            status_str = ("##################################################\n"
                          "## Hop-client: dumping unknown message as json ###\n"
                          "##################################################")
            json_dump = True

    print(status_str)
    
    if json_dump:
        print(json.dumps(msg))
    else:
        print(str(gcn))


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
        default=10,
        help="Specifies the time (in seconds) to wait for messages before timing out; "
        "specify 'None' to wait indefinitely.  Default: 10 seconds",
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
    t = args.timeout
    timeout = None if (t is None or t == 'None') else float(t)

    # read from topic

    # assume json format for the gcn
    gcn_format = "json"

    stream = Stream(format=gcn_format, config=config, start_at=start_offset)
    with stream.open(args.url, "r") as s:
        for msg in s(timeout=timeout):
            print_msg(msg, args.json)
