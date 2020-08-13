from unittest.mock import patch, mock_open

import pytest

from hop import auth
from hop import configure
import subprocess
import os


def test_load_auth(auth_config):
    with patch("builtins.open", mock_open(read_data=auth_config)) as mock_file:

        # check error handling
        with pytest.raises(FileNotFoundError):
            auth.load_auth()

        # check auth loads correctly
        with patch("os.path.exists") as mock_exists:
            mock_exists.return_value = True
            auth.load_auth()


def test_setup_auth():

    credentials_file = "credentials.csv"
    username = "scimma"
    password = "scimmapass"
    with open(credentials_file, "w") as f:
        f.write("username,password\n")
        f.write(username + "," + password + "\n")

    # check on new configuration file is written using credential file
    process = subprocess.Popen(["hop", "configure", "setup", "--import", credentials_file],
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output, error = process.communicate()
    assert "hop : INFO : Generated configuration at:" in output.decode("utf-8")
    configuration_file = configure.get_config_path()
    cf = open(configuration_file, "r")
    config_file_text = cf.read()
    assert username in config_file_text
    assert password in config_file_text
    os.remove(credentials_file)

    # hop configure setup (need --force)
    process = subprocess.Popen(["hop", "configure", "setup"],
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output, error = process.communicate()
    warning_message = "hop : WARNING : Configuration already exists, overwrite file with --force"
    assert warning_message in output.decode("utf-8")


def test_no_command_configure():
    process = subprocess.Popen(["hop", "configure"],
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output, error = process.communicate()
    warning_message = (
        "usage: hop configure [-h] <command> ...\n"
        "hop configure: error: the following arguments are required: <command>"
    )
    assert warning_message in output.decode("utf-8")
