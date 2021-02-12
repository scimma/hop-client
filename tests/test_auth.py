from unittest.mock import patch, mock_open
from contextlib import contextmanager

import pytest

from hop import auth
from hop import configure
import os
import stat


@contextmanager
def temp_environ(**vars):
    """
    A simple context manager for temporarily setting environment variables

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


@contextmanager
def temp_config(data, perms=stat.S_IRUSR | stat.S_IWUSR):
    """
    A context manager which creates a temporary config file with specified data and permissions

    Args:
        data: the data to be written to the file
        perms: the permissions which should be set on the file.
            The default value is to use the standard, safe permissions

    Returns:
        None
    """
    config_path = configure.get_config_path()
    os.makedirs(os.path.dirname(config_path), exist_ok=True)
    config_file = open(config_path, mode='w')
    os.chmod(config_path, perms)
    config_file.write(data)
    config_file.close()
    try:
        yield  # no value needed
    finally:
        # remove file
        os.remove(config_path)


def test_load_auth(auth_config, tmpdir):
    with temp_environ(XDG_CONFIG_HOME=str(tmpdir)), temp_config(auth_config):
        auth.load_auth()


def test_load_auth_non_existent(auth_config, tmpdir):
    with temp_environ(XDG_CONFIG_HOME=str(tmpdir)), \
            pytest.raises(FileNotFoundError):
        auth.load_auth()


def test_load_auth_bad_perms(auth_config, tmpdir):
    with temp_environ(XDG_CONFIG_HOME=str(tmpdir)):
        for bad_perm in [stat.S_IRGRP, stat.S_IWGRP, stat.S_IXGRP,
                         stat.S_IROTH, stat.S_IWOTH, stat.S_IXOTH]:
            with temp_config(auth_config, bad_perm), pytest.raises(RuntimeError):
                auth.load_auth()


def test_load_auth_malformed(tmpdir):
    with temp_environ(XDG_CONFIG_HOME=str(tmpdir)):
        missing_username = """
                           [auth]
                           password = "password"
                           extra = "stuff"
                           """
        with temp_config(missing_username), pytest.raises(KeyError):
            auth.load_auth()

        missing_password = """
                           [auth]
                           username = "username"
                           extra = "stuff"
                       """
        with temp_config(missing_password), pytest.raises(KeyError):
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


def test_setup_auth(script_runner, tmpdir):
    with temp_environ(XDG_CONFIG_HOME=str(tmpdir)):
        credentials_file = tmpdir / "credentials.csv"
        username = "scimma"
        password = "scimmapass"
        with open(credentials_file, "w") as f:
            f.write("username,password\n")
            f.write(username + "," + password + "\n")

        # check on new configuration file is written using credential file
        ret1 = script_runner.run("hop", "configure", "setup", "--import", str(credentials_file))
        assert ret1.success
        assert "hop : INFO : Generated configuration at:" in ret1.stderr
        configuration_file = configure.get_config_path()
        cf = open(configuration_file, "r")
        config_file_text = cf.read()
        assert username in config_file_text
        assert password in config_file_text
        os.remove(credentials_file)

        # hop configure setup (need --force)
        warning_message = \
            "hop : WARNING : Configuration already exists, overwrite file with --force"
        ret2 = script_runner.run("hop", "configure", "setup")
        assert warning_message in ret2.stderr


def test_no_command_configure(script_runner):
    warning_message = (
        "usage: hop configure [-h] <command> ...\n"
        "hop configure: error: the following arguments are required: <command>"
    )
    ret = script_runner.run("hop", "configure")
    assert not ret.success
    assert warning_message in ret.stderr
