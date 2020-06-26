#!/usr/bin/env python

__author__ = "Bryce Cousins (bfc5288@psu.edu)"
__description__ = "tools to receive and parse messages"


import argparse
import json

from . import cli
from . import io
from .models import GCNCircular, VOEvent, MessageBlob


def classify_message(message):
    """Check the format of a message obtained from an ADC stream and
    use it to instantiate a data model corresponding to that format.

    Args:
      message:       wrapped message from an ADC stream

    Returns:
      message_model: dataclass model object for the message

    """

    try:
        fmt = message["format"]
        content = message["content"]
    except TypeError:
        raise ValueError("Message is not wrapped with format/content keys")
    except KeyError:
        raise KeyError("Message does not contain format/content keys")

    # create map of message formats and dataclass models for parsing
    model_creator = {
        "circular": GCNCircular,
        "voevent": VOEvent,
        "blob": MessageBlob,
    }

    if fmt in model_creator:
        creator = model_creator[fmt]
        message_model = creator(**content)
    else:
        raise ValueError(f"Message format {fmt} not recognized")

    return message_model


def print_message(message_model, json_dump=False):
    """Print the content of a message.

    Args:
      message_model: dataclass model object for a message
      json_dump: boolean indicating whether to print as raw json

    Returns:
      None
    """

    # print the message content
    if json_dump:
        print(json.dumps(message_model.asdict()))
    else:
        print(str(message_model))


# ------------------------------------------------
# -- main


def _add_parser_args(parser):
    cli.add_url_opts(parser)

    # consumer options
    parser.add_argument(
        "-s",
        "--start-at",
        choices=io.StartPosition.__members__,
        default=str(io.StartPosition.LATEST),
        help="Set the message offset offset to start at. Default: LATEST.",
    )
    parser.add_argument(
        "-p",
        "--persist",
        action="store_true",
        help="If set, persist or listen to messages indefinitely. "
             "Otherwise, will stop listening when EOS is received.",
    )
    parser.add_argument(
        "-j", "--json", help="Request message output as raw json", action="store_true",
    )


def _main(args=None):
    """Receive and parse messages.

    """
    if not args:
        parser = argparse.ArgumentParser()
        _add_parser_args(parser)
        args = parser.parse_args()

    # read from topic
    stream = io.Stream(start_at=args.start_at, persist=args.persist)
    with stream.open(args.url, "r") as s:
        for message in s:
            print_message(message, args.json)
