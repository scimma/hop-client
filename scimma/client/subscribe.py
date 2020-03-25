#!/usr/bin/env python

__author__ = "Bryce Cousins (bfc5288@psu.edu)"
__description__ = "tools to receive and parse GCN circulars"


import argparse
import email

import json

from .io import Stream
from .models import GCNCircular

def print_gcn(gcn_dict,json_dump=False):
    '''Parse a gcn dictionary and print to stdout.

    Args:
      gcn_dict:  the dictionary object containing a GCN-formatted message
    
    Returns:
      None

    '''
    if json_dump:
        print(json.dumps(gcn_dict))
    else:
        gcn_headers = gcn_dict['header']
        gcn_body = gcn_dict['body']

        for key in gcn_headers:
            line = key.upper() + ':\t' + gcn_headers[key]
            print(line)

        for line in gcn_body.splitlines():
            print(line)

def print_gcn(gcn_dict,json_dump=False):
    '''
    '''

    if json_dump:
        print(json.dumps(gcn_dict))
    else:
        #print(json.dumps(gcn_dict['header'], indent=0))
        #print(json.dumps(gcn_dict['body'], indent=0))
        gcn_headers = gcn_dict['header']
        gcn_body = gcn_dict['body']

        for key in gcn_headers:
            print(key, ': ', gcn_headers[key])
        print('')    #newline for formatting

        for line in gcn_body.splitlines():
            print(line)

# ------------------------------------------------
# -- main


def _add_parser_args(parser):
    parser.add_argument(
        "-b",
        "--broker-url",
        required=True,
        help="Sets the broker URL (kafka://host[:port]/topic) from which to receive GCNs.",
    )
    parser.add_argument(
        "-j", "--json", help="A test argument for raw json printing",
    )

    # configuration options
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

    # consumer options
    parser.add_argument(
        "-j", "--json", help="Request gcn output as raw json",
        action='store_true',
    )
    parser.add_argument(
        "-e", "--earliest", help="Request to stream from the earliest available Kafka offset",
        action='store_true',
    )
    parser.add_argument(
        "-t", "--timeout", help="Specifies the time (in seconds) to wait for new messages.",
    )

def _main(args=None):
    """Receive and parse GCN circulars.

    """
    if not args:
        parser = argparse.ArgumentParser()
        _add_parser_args(parser)
        args = parser.parse_args()

    # load config if specified
    if args.config_file:
        config = args.config_file
    elif args.config:
        config = {opt[0]: opt[1] for opt in (kv.split("=") for kv in args.config)}
    else:
        config = None

    # load consumer options

    # defaults:
    start_offset = 'latest'
    timeout = 10
    json_dump = False

    if args.json:
        json_dump = True
    if args.earliest:
        start_offset = 'earliest'
    if args.timeout:
        timeout = int(args.timeout)

    # read from topic

    # assume json format for the gcn
    gcn_format = 'json'
    
    stream = Stream(format=gcn_format, config=config, start_at=start_offset)
    with stream.open(args.broker_url, "r") as s:
        for _,gcn_dict in s(timeout=timeout):
            print_gcn(gcn_dict,json_dump)
