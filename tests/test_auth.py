from unittest.mock import patch, mock_open
from contextlib import contextmanager

import pytest

from hop import auth
from hop import configure
import subprocess
import os


@contextmanager
def temp_environ(**vars):
    """
    A simple context manager for temprarily setting environment variables

    Kwargs:
        variables to be set and their values

    Returns:
        None
    """
    from os import environ
    original = dict(environ)
    os.environ.update(vars)
    try:
        yield  # no value needed
    finally:
        # restore original data
        os.environ.clear()
        os.environ.update(original)


def test_load_auth(auth_config, tmpdir):
    with patch("builtins.open", mock_open(read_data=auth_config)) as mock_file, \
            temp_environ(XDG_CONFIG_HOME=str(tmpdir)):

        # check error handling
        with pytest.raises(FileNotFoundError):
            auth.load_auth()

        # check auth loads correctly
        with patch("os.path.exists") as mock_exists:
            mock_exists.return_value = True
            auth.load_auth()


def test_load_auth_malformed():
    missing_username = """
                       [auth]
                       password = "password"
                       extra = "stuff"
                       """
    with patch("builtins.open", mock_open(read_data=missing_username)) as mock_file, \
            patch("os.path.exists") as mock_exists, \
            pytest.raises(KeyError):
        auth.load_auth()

    missing_password = """
                       [auth]
                       username = "username"
                       extra = "stuff"
                       """
    with patch("builtins.open", mock_open(read_data=missing_password)) as mock_file, \
            patch("os.path.exists") as mock_exists, \
            pytest.raises(KeyError):
        auth.load_auth()


def test_load_auth_options(auth_config):
    # SSL should be used by default
    # The default mechanism should be SCRAM_SHA_512
    with patch("builtins.open", mock_open(read_data=auth_config)) as mock_file, \
            patch("os.path.exists") as mock_exists, \
            patch("hop.auth.Auth") as auth_mock:
        auth.load_auth()
        assert auth_mock.called_with(ssl=True)
        from adc.auth import SASLMethod
        assert auth_mock.called_with(mechanism=SASLMethod.SCRAM_SHA_512)

    # But it should be possible to disable SSL
    use_plaintext = """
                       [auth]
                       username = "username"
                       password = "password"
                       protocol = "SASL_PLAINTEXT"
                       """
    with patch("builtins.open", mock_open(read_data=use_plaintext)) as mock_file, \
            patch("os.path.exists") as mock_exists, \
            patch("hop.auth.Auth") as auth_mock:
        auth.load_auth()
        assert auth_mock.called_with(ssl=False)

    # An SSL CA data path should be honored
    with_ca_data = """
                   [auth]
                   username = "username"
                   password = "password"
                   ssl_ca_location = "/foo/bar/baz"
                   """
    with patch("builtins.open", mock_open(read_data=with_ca_data)) as mock_file, \
            patch("os.path.exists") as mock_exists, \
            patch("hop.auth.Auth") as auth_mock:
        auth.load_auth()
        assert auth_mock.called_with(ssl_ca_location="/foo/bar/baz")

    # Alternate mechanisms should be honored
    plain_mechanism = """
                      [auth]
                      username = "username"
                      password = "password"
                      mechanism = "PLAIN"
                      """
    with patch("builtins.open", mock_open(read_data=plain_mechanism)) as mock_file, \
            patch("os.path.exists") as mock_exists, \
            patch("hop.auth.Auth") as auth_mock:
        auth.load_auth()
        assert auth_mock.called_with(mechanism=SASLMethod.PLAIN)


def test_setup_auth(tmpdir):
    with temp_environ(XDG_CONFIG_HOME=str(tmpdir)):
        credentials_file = tmpdir / "credentials.csv"
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
        warning_message = \
            "hop : WARNING : Configuration already exists, overwrite file with --force"
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
