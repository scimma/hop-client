import pytest
import os
from conftest import temp_environ

from hop import configure


def test_get_config_path(tmpdir):
    with temp_environ(HOME=str(tmpdir)):
        if "XDG_CONFIG_HOME" in os.environ:
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


def test_get_config_path_auth(tmpdir):
    with temp_environ(HOME=str(tmpdir)):
        if "XDG_CONFIG_HOME" in os.environ:
            # this change will revert at the end of the with block
            del os.environ["XDG_CONFIG_HOME"]

        # with HOME set but not XDG_CONFIG_HOME the config location should resolve to inside
        # ${HOME}/.config
        expected_path = os.path.join(tmpdir, ".config", "hop", "auth.toml")
        config_loc = configure.get_config_path("auth")
        assert config_loc == expected_path

        with temp_environ(XDG_CONFIG_HOME=str(tmpdir)):
            # with XDG_CONFIG_HOME set, no .config path component should be assumed
            expected_path = os.path.join(tmpdir, "hop", "auth.toml")
            config_loc = configure.get_config_path("auth")
            assert config_loc == expected_path


def test_get_config_path_invalid(tmpdir):
    with pytest.raises(ValueError):
        configure.get_config_path("hairstyle")


def test_no_command_configure(script_runner):
    warning_message = (
        "hop configure: error: the following arguments are required: <command>"
    )
    ret = script_runner.run("hop", "configure")
    assert not ret.success
    assert warning_message in ret.stderr
