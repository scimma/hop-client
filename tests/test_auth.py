from unittest.mock import patch

import pytest

from hop import auth
from hop import configure
import os
import stat

from conftest import temp_environ, temp_config


def test_load_auth(auth_config, tmpdir):
    with temp_config(tmpdir, auth_config) as config_dir, temp_environ(XDG_CONFIG_HOME=config_dir):
        auth_data = auth.load_auth()
        assert auth_data.username == "username"


def test_load_auth_non_existent(auth_config, tmpdir):
    with temp_environ(XDG_CONFIG_HOME=str(tmpdir)), \
            pytest.raises(FileNotFoundError):
        auth.load_auth()


def test_load_auth_bad_perms(auth_config, tmpdir):
    for bad_perm in [stat.S_IRGRP, stat.S_IWGRP, stat.S_IXGRP,
                     stat.S_IROTH, stat.S_IWOTH, stat.S_IXOTH]:
        with temp_config(tmpdir, auth_config, bad_perm) as config_dir, \
                temp_environ(XDG_CONFIG_HOME=config_dir), pytest.raises(RuntimeError):
            auth.load_auth()


def test_load_auth_malformed(tmpdir):
    missing_username = """
                       [auth]
                       password = "password"
                       extra = "stuff"
                       """
    with temp_config(tmpdir, missing_username) as config_dir, \
            temp_environ(XDG_CONFIG_HOME=config_dir), pytest.raises(KeyError):
        auth.load_auth()

    missing_password = """
                       [auth]
                       username = "username"
                       extra = "stuff"
                   """
    with temp_config(tmpdir, missing_password) as config_dir, \
            temp_environ(XDG_CONFIG_HOME=config_dir), pytest.raises(KeyError):
        auth.load_auth()


def test_load_auth_options(auth_config, tmpdir):
    # SSL should be used by default
    # The default mechanism should be SCRAM_SHA_512
    with temp_config(tmpdir, auth_config) as config_dir, \
            temp_environ(XDG_CONFIG_HOME=config_dir), patch("hop.auth.Auth") as auth_mock:
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
    with temp_config(tmpdir, use_plaintext) as config_dir, \
            temp_environ(XDG_CONFIG_HOME=config_dir), patch("hop.auth.Auth") as auth_mock:
        auth.load_auth()
        assert auth_mock.called_with(ssl=False)

    # An SSL CA data path should be honored
    with_ca_data = """
                   [auth]
                   username = "username"
                   password = "password"
                   ssl_ca_location = "/foo/bar/baz"
                   """
    with temp_config(tmpdir, with_ca_data) as config_dir, \
            temp_environ(XDG_CONFIG_HOME=config_dir), patch("hop.auth.Auth") as auth_mock:
        auth.load_auth()
        assert auth_mock.called_with(ssl_ca_location="/foo/bar/baz")

    # Alternate mechanisms should be honored
    plain_mechanism = """
                      [auth]
                      username = "username"
                      password = "password"
                      mechanism = "PLAIN"
                      """
    with temp_config(tmpdir, plain_mechanism) as config_dir, \
            temp_environ(XDG_CONFIG_HOME=config_dir), patch("hop.auth.Auth") as auth_mock:
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
