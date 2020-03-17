#!/usr/bin/env python

__author__ = "Bryce Cousins (bfc5288@psu.edu)"
__description__ = "a module that tests subscribe utilities"

import io
import codecs
from contextlib import redirect_stdout
from pathlib import Path

from unittest.mock import patch, mock_open

from hop import subscribe


def test_print_gcn(circular_text, circular_msg):
    with patch("builtins.open", mock_open(read_data=circular_text)) as mock_file:

        # parse the sample GCN from stdout
        f = io.StringIO()
        with redirect_stdout(f):
            subscribe.print_gcn(circular_msg, json_dump=False)

        # verify circular_msg was read in
        assert open(circular_msg).read() == circular_text
        mock_file.assert_called_with(circular_msg)

        # read stdout from beginning
        f.seek(0)

        # extract GCN string from stdout
        gcn_stdout_list = f.readlines()
        gcn_stdout_str = "".join(gcn_stdout_list)

        # read in expected data txt
        shared_datadir = Path("tests/data")
        expected_raw = (
            shared_datadir / "expected_data" / "gcn_circular_stdout.txt"
        ).read_text()

        # use codec to decode the expected output to leave newline/tab characters intact
        expected = codecs.unicode_escape_decode(expected_raw)[0][:-1]  # remove ending newline char

        # verify printed GCN structure is correct
        assert gcn_stdout_str == expected
