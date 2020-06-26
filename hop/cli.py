#!/usr/bin/env python

__author__ = "Patrick Godwin (patrick.godwin@psu.edu)"
__description__ = "a module for CLI utilities"


def add_url_opts(parser):
    parser.add_argument(
        "url",
        metavar="URL",
        help="Sets the URL (kafka://host[:port]/topic) to publish messages to.",
    )
