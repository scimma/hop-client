import dataclasses
from io import StringIO
import pytest
import toml
import os
from unittest.mock import MagicMock, patch
from conftest import temp_config, temp_environ

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


def test_Config_load():
    # can load from a string
    c1 = configure.Config.load(StringIO("[config]"))
    for field in dataclasses.fields(configure.Config):
        assert getattr(c1, field.name) == field.default, \
            "default values should be used when not specified"

    non_default = StringIO()
    non_default.write("[config]\n")
    for field in dataclasses.fields(configure.Config):
        if field.type is bool:
            non_default.write(f"{field.name} = {str(not field.default).lower()}\n")
    c2 = configure.Config.load(non_default.getvalue())
    for field in dataclasses.fields(configure.Config):
        if field.type is bool:
            assert getattr(c2, field.name) != field.default, \
                "non-default values should be read correctly"
        else:
            assert getattr(c2, field.name) == field.default, \
                "default values should be used when not specified"

    with pytest.raises(RuntimeError) as ex:
        configure.Config.load("valid = false\n")
    assert "configuration data has no config section" in str(ex)

    with pytest.raises(RuntimeError) as ex:
        configure.Config.load('{"this": "is not TOML", "json": "yes"}')
    assert "configuration data is malformed" in str(ex)


def test_Config_roundtrip():
    out_buf = StringIO()
    c1 = configure.Config()
    # set some non-default values
    for field in dataclasses.fields(configure.Config):
        if field.type is bool:
            setattr(c1, field.name, not field.default)
    c1.save(out_buf)
    in_buf = StringIO(out_buf.getvalue())
    c2 = configure.Config.load(in_buf)
    assert c1 == c2


def test_parse_bool():
    assert configure._parse_bool("TrUe")
    assert configure._parse_bool("Yes")
    assert configure._parse_bool("oN")
    assert configure._parse_bool("1")
    assert not configure._parse_bool("fAlsE")
    assert not configure._parse_bool("No")
    assert not configure._parse_bool("oFF")
    assert not configure._parse_bool("0")
    with pytest.raises(ValueError) as ex:
        configure._parse_bool("fish")
    assert "Invalid boolean value" in str(ex)
    assert "fish" in str(ex)


def test_load_config(general_config, tmpdir):
    with temp_config(tmpdir, general_config) as conf_dir, temp_environ(XDG_CONFIG_HOME=conf_dir):
        config = configure.load_config()
        assert not config.fetch_external
        assert not config.automatic_offload

    # if there is no file, we should construct a default object
    with temp_environ(XDG_CONFIG_HOME=str(tmpdir)):
        config = configure.load_config()
        assert config == configure.Config()

    # when specified, we should load from a non-default path
    with temp_config(tmpdir, general_config) as conf_dir, temp_environ(XDG_CONFIG_HOME=conf_dir):
        alt_path = f"{tmpdir}/other.toml"
        with open(alt_path, 'w') as f:
            toml.dump({"config": {"fetch_external": not configure.Config.fetch_external}}, f)
        config = configure.load_config(alt_path)
        os.remove(alt_path)
        assert not config.fetch_external
        assert config.automatic_offload

    # if the specified file exists but cannot be read,
    # we should construct a default object and emit a warning
    with temp_environ(XDG_CONFIG_HOME=str(tmpdir)), \
            patch("hop.configure.os.path.exists", MagicMock(return_value=True)), \
            patch("builtins.open", MagicMock(side_effect=PermissionError())):
        with pytest.warns(Warning):
            config = configure.load_config()
        assert config == configure.Config()

    # malformed data should cause a meaningful error
    with temp_config(tmpdir, "BAD\tTOML") as conf_dir, temp_environ(XDG_CONFIG_HOME=conf_dir):
        with pytest.raises(RuntimeError) as ex:
            config = configure.load_config()
        assert f"Error loading {conf_dir}" in str(ex)


def test_load_config_env(tmpdir):
    with temp_environ(XDG_CONFIG_HOME=str(tmpdir), HOP_FETCH_EXTERNAL="no"):
        config = configure.load_config()
        assert not config.fetch_external
        assert config.automatic_offload

    with temp_environ(XDG_CONFIG_HOME=str(tmpdir), HOP_FETCH_EXTERNAL="elk"):
        with pytest.raises(RuntimeError) as ex:
            config = configure.load_config()
        assert "Error parsing configuration variable HOP_FETCH_EXTERNAL" in str(ex)
        assert ex.value.__cause__ is not None
        assert "Invalid boolean value" in str(ex.value.__cause__)

    @dataclasses.dataclass
    class FakeConfig:
        number: int = 2

    with patch("hop.configure.Config", FakeConfig):
        with temp_environ(XDG_CONFIG_HOME=str(tmpdir), HOP_NUMBER="8"):
            config = configure.load_config()
            assert config.number == 8

        with temp_environ(XDG_CONFIG_HOME=str(tmpdir), HOP_NUMBER="rhino"):
            with pytest.raises(RuntimeError) as ex:
                config = configure.load_config()
            assert "Error parsing configuration variable HOP_NUMBER" in str(ex)
            assert ex.value.__cause__ is not None
            assert "invalid literal for int() with base 10: 'rhino'" in str(ex.value.__cause__)


def test_write_config_value(general_config, tmpdir):
    os.makedirs(f"{tmpdir}/hop", exist_ok=True)

    with temp_environ(XDG_CONFIG_HOME=str(tmpdir)):
        value = not configure.Config.fetch_external
        configure.write_config_value("fetch_external", value)
        config_path = configure.get_config_path("general")
        assert os.path.exists(config_path)
        with open(config_path, 'r') as f:
            data = toml.load(f)
        os.remove(config_path)
        assert "config" in data
        assert "fetch_external" in data["config"]
        assert data["config"]["fetch_external"] == value

    with temp_environ(XDG_CONFIG_HOME=str(tmpdir), HOP_AUTOMATIC_OFFLOAD="0"):
        value = not configure.Config.fetch_external
        configure.write_config_value("fetch_external", value)
        config_path = configure.get_config_path("general")
        assert os.path.exists(config_path)
        with open(config_path, 'r') as f:
            data = toml.load(f)
        os.remove(config_path)
        assert "config" in data
        assert "fetch_external" in data["config"]
        assert "automatic_offload" not in data["config"], "environment variables must be ignored"

    # Valid TOML with the wrong data should yield an error
    with temp_environ(XDG_CONFIG_HOME=str(tmpdir)):
        config_path = configure.get_config_path("general")
        with open(config_path, 'w') as f:
            toml.dump({"not-config": 17}, f)
        with open(config_path, 'r') as f:
            data = f.read()
        print(data)
        with pytest.raises(RuntimeError) as ex:
            configure.write_config_value("fetch_external", True)
        assert "has no config section" in str(ex)

    with temp_environ(XDG_CONFIG_HOME=str(tmpdir)):
        config_path = configure.get_config_path("general")
        with open(config_path, 'w') as f:
            f.write('{"this": "is not TOML", "json": "yes"}')
        with pytest.raises(RuntimeError) as ex:
            configure.write_config_value("fetch_external", True)
        assert "is malformed" in str(ex)


def test_no_command_configure(script_runner):
    warning_message = (
        "hop configure: error: the following arguments are required: <command>"
    )
    ret = script_runner.run(["hop", "configure"])
    assert not ret.success
    assert warning_message in ret.stderr
