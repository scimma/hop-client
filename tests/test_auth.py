from unittest.mock import patch, mock_open

import pytest

from hop import auth
import subprocess
import os


def test_load_auth():
    with patch("builtins.open", mock_open(read_data=auth.DEFAULT_AUTH_CONFIG)) as mock_file:

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

    # check that configuration file will be written
    result = subprocess.Popen(["hop", "auth", "setup"],
                              stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT)
    output, error = result.communicate()
    assert "Your configuration file has been written at" in output.decode("utf-8")

    # check on --force is required
    result = subprocess.Popen(["hop", "auth", "setup"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output, error = result.communicate()
    assert "Configuration already exists, overwrite file with --force" in output.decode("utf-8")

    # check on new configuration file is written using credential file
    result = subprocess.Popen(["hop", "auth", "setup", "--force", "-c", credentials_file],
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output, error = result.communicate()
    assert "Your configuration file has been written at" in output.decode("utf-8")
    configuration_file = auth.get_auth_path()
    cf = open(configuration_file, "r")
    config_file_text = cf.read()
    assert username in config_file_text
    assert password in config_file_text

    os.remove(credentials_file)
