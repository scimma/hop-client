#!/usr/bin/env python

__author__ = "Bryce Cousins (bfc5288@psu.edu)"
__description__ = "a module that tests subscribe utilities"

import io
import codecs
from contextlib import redirect_stdout
from pathlib import Path

from unittest.mock import patch, MagicMock
import pytest

from hop import subscribe


# test the subscribe printer for each message type: voevent, circular
@pytest.mark.parametrize("msg_type", ["voevent", "circular"])
def test_print_gcn(msg_type):
    if msg_type == "voevent":
        model_type = "VOEvent"
        test_file = "example_voevent.xml"
    elif msg_type == "circular":
        model_type = "GCNCircular"
        test_file = "example_gcn.gcn3"

    shared_datadir = Path("tests/data")

    test_content = (shared_datadir / "test_data" / test_file).read_text()

    # control the output of the hop.model's print method with mocking
    with patch(f"hop.models.{model_type}", MagicMock()) as mock_model:
        mock_model.__str__.return_value = test_content

        f = io.StringIO()
        with redirect_stdout(f):
            subscribe.print_gcn(mock_model, json_dump=False)

        # read stdout from beginning
        f.seek(0)

        # extract GCN string from stdout
        test_gcn_stdout_list = f.readlines()
        test_gcn_stdout_str = "".join(test_gcn_stdout_list)

        # read in expected stdout text
        expected_gcn_raw = (
            shared_datadir / "expected_data" / (test_file + ".stdout")
        ).read_text()

        # use codec to decode the expected output to leave newline/tab characters intact
        expected_gcn_stdout = codecs.unicode_escape_decode(expected_gcn_raw)[0]

        # verify printed GCN structure is correct
        print(test_gcn_stdout_str)

        assert test_gcn_stdout_str == expected_gcn_stdout
