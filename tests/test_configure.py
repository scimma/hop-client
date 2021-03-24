import pytest
import os
from unittest.mock import patch, MagicMock
from conftest import temp_environ

from hop import configure


def check_config_file(config_path, username, password):
    assert os.path.exists(config_path)
    assert os.stat(config_path).st_mode & 0o7777 == 0o600
    cf = open(config_path, "r")
    config_file_text = cf.read()
    assert username in config_file_text
    assert password in config_file_text


def test_get_config_path(tmpdir):
    with temp_environ(HOME=str(tmpdir)):
        # this change will revert at the end of the with block
        del os.environ["XDG_CONFIG_HOME"]

        # with HOME set but not XDG_CONFIG_HOME the config location should resolve to inside
        # ${HOME}/.config
        expected_path = os.path.join(tmpdir, ".config", "hop", "config.toml")
        config_loc = configure.get_config_path()
        assert config_loc == expected_path

        with temp_environ(XDG_CONFIG_HOME=str(tmpdir)):
            # with XDG_CONFIG_HOME set, no .config path component should be assumed
            expected_path = os.path.join(tmpdir, "hop", "config.toml")
            config_loc = configure.get_config_path()
            assert config_loc == expected_path


def test_write_config_file(tmpdir):
    config_file = tmpdir + "/config"
    username = "scimma"
    password = "scimmapass"
    configure.write_config_file(config_file, username, password)
    check_config_file(config_file, username, password)


def test_set_up_configuration_interactive(tmpdir):
    config_file = tmpdir + "/config"
    username = "scimma"
    password = "scimmapass"
    with patch("getpass.getpass", MagicMock(return_value=password)), \
            patch("hop.configure.input", MagicMock(return_value=username)):
        configure.set_up_configuration(config_file, csv_file=None)
    check_config_file(config_file, username, password)


def test_set_up_configuration_csv(tmpdir):
    config_file = tmpdir + "/config"
    csv_file = tmpdir + "/input.csv"
    username = "scimma"
    password = "scimmapass"
    with open(csv_file, "w") as f:
        f.write("username,password\n")
        f.write(username + "," + password + "\n")
    configure.set_up_configuration(config_file, csv_file=csv_file)
    check_config_file(config_file, username, password)


def test_set_up_configuration_missing_csv():
    # send output to /dev/null, since there shouldn't be any
    with pytest.raises(FileNotFoundError):
        configure.set_up_configuration(
            "/dev/null", csv_file="file_which_does_not_exist.csv"
        )
