import pytest
import os
from unittest.mock import patch, MagicMock
from conftest import temp_environ, temp_config

from hop import configure
from hop import auth


def check_credential_file(config_path, cred):
    assert os.path.exists(config_path)
    assert os.stat(config_path).st_mode & 0o7777 == 0o600
    cf = open(config_path, "r")
    config_file_text = cf.read()
    assert cred.username in config_file_text
    assert cred._config["sasl.password"] in config_file_text
    if len(cred.hostname) > 0:
        assert cred.hostname in config_file_text


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


def test_read_new_credential_csv(tmpdir):
    csv_file = tmpdir + "/cred.csv"

    # read from a csv file with no hostname
    with open(csv_file, "w") as f:
        f.write("username,password\n")
        f.write("user,pass")
    new_cred = configure.read_new_credential(csv_file)
    assert new_cred.username == "user"
    assert new_cred._config["sasl.password"] == "pass"
    assert new_cred.hostname == ""

    # read from a csv file with a hostname
    with open(csv_file, "w") as f:
        f.write("username,password,hostname\n")
        f.write("user2,pass2,example.com")
    new_cred = configure.read_new_credential(csv_file)
    assert new_cred.username == "user2"
    assert new_cred._config["sasl.password"] == "pass2"
    assert new_cred.hostname == "example.com"


def test_read_new_credential_csv_malformed(tmpdir):
    csv_file = tmpdir + "/cred.csv"

    # nonexistent file => FileNotFoundError
    with pytest.raises(FileNotFoundError):
        configure.read_new_credential(csv_file)

    # no username => KeyError
    with open(csv_file, "w") as f:
        f.write("password,hostname\n")
        f.write("pass,example.com")
    with pytest.raises(KeyError):
        configure.read_new_credential(csv_file)

    # no password => KeyError
    with open(csv_file, "w") as f:
        f.write("username,hostname\n")
        f.write("user,example.com")
    with pytest.raises(KeyError):
        configure.read_new_credential(csv_file)


def test_read_new_credential_interactive(tmpdir):
    username = "foo"
    password = "bar"
    for hostname in ["", "example.com"]:
        with patch("getpass.getpass", MagicMock(return_value=password)), \
                patch("hop.configure.input", MagicMock(side_effect=[username, hostname])):
            new_cred = configure.read_new_credential()
            assert new_cred.username == username
            assert new_cred._config["sasl.password"] == password
            assert new_cred.hostname == hostname


def test_read_new_credential_interactive_invalid(tmpdir):
    username = "foo"
    password = "bar"
    hostname = "example.com"

    # missing username
    with patch("getpass.getpass", MagicMock(return_value=password)), \
            patch("hop.configure.input", MagicMock(side_effect=["", hostname])), \
            pytest.raises(RuntimeError) as err:
        configure.read_new_credential()
    assert err.value.args[0] == "Username may not be empty"

    # missing password
    with patch("getpass.getpass", MagicMock(return_value="")), \
            patch("hop.configure.input", MagicMock(side_effect=[username, hostname])), \
            pytest.raises(RuntimeError) as err:
        configure.read_new_credential()
    assert err.value.args[0] == "Password may not be empty"


def test_write_config_data(tmpdir):
    config_file = tmpdir + "/config"
    username = "scimma"
    password = "scimmapass"
    configure.write_auth_data(config_file, [auth.Auth(username, password)])
    check_credential_file(config_file, auth.Auth(username, password))


def test_set_up_configuration_interactive(tmpdir):
    config_file = tmpdir + "/config"
    username = "scimma"
    password = "scimmapass"
    with patch("getpass.getpass", MagicMock(return_value=password)), \
            patch("hop.configure.input", MagicMock(return_value=username)):
        configure.set_up_configuration(config_file, csv_file=None)
    check_credential_file(config_file, auth.Auth(username, password))


def test_set_up_configuration_csv(tmpdir):
    config_file = tmpdir + "/config"
    csv_file = tmpdir + "/input.csv"
    username = "scimma"
    password = "scimmapass"
    with open(csv_file, "w") as f:
        f.write("username,password\n")
        f.write(username + "," + password + "\n")
    configure.set_up_configuration(config_file, csv_file=csv_file)
    check_credential_file(config_file, auth.Auth(username, password))


def test_set_up_configuration_missing_csv():
    # send output to /dev/null, since there shouldn't be any
    with pytest.raises(FileNotFoundError):
        configure.set_up_configuration(
            "/dev/null", csv_file="file_which_does_not_exist.csv"
        )


def test_list_credentials(tmpdir, capsys):
    with patch("hop.auth.load_auth", MagicMock(return_value=[])):
        configure.list_credentials()
        captured = capsys.readouterr()
        assert len(captured.out) == 0

    # lie about output going to a TTY to check more user-friendly message used then
    with patch("hop.auth.load_auth", MagicMock(return_value=[])), \
            patch("os.isatty", MagicMock(return_value=True)):
        configure.list_credentials()
        captured = capsys.readouterr()
        assert "No credentials" in captured.out

    short_cred = auth.Auth("user1", "pass1")
    long_cred = auth.Auth("user2", "pass2", "host2")

    with patch("hop.auth.load_auth", MagicMock(return_value=[short_cred])):
        configure.list_credentials()
        captured = capsys.readouterr()
        assert short_cred.username in captured.out
        assert short_cred._config["sasl.password"] not in captured.out
        assert "for" not in captured.out

    with patch("hop.auth.load_auth", MagicMock(return_value=[short_cred, long_cred])):
        configure.list_credentials()
        captured = capsys.readouterr()
        assert short_cred.username in captured.out
        assert long_cred.username in captured.out
        assert long_cred._config["sasl.password"] not in captured.out
        assert long_cred.hostname in captured.out


def test_add_credential_to_empty(tmpdir):
    new_cred = auth.Auth("user", "pass")

    with temp_environ(HOME=str(tmpdir)), \
            patch("hop.configure.read_new_credential", MagicMock(return_value=new_cred)):
        args = MagicMock()
        args.cred_file = None
        args.force = False
        configure.add_credential(args)
        check_credential_file(configure.get_config_path("auth"), new_cred)


def test_add_credential_to_nonempty(auth_config, tmpdir):
    old_cred = auth.Auth("username", "password")
    new_cred = auth.Auth("other_user", "other_pass")

    with temp_config(tmpdir, auth_config) as config_dir, temp_environ(HOME=config_dir), \
            patch("hop.configure.read_new_credential", MagicMock(return_value=new_cred)):
        args = MagicMock()
        args.cred_file = None
        args.force = False
        configure.add_credential(args)
        check_credential_file(configure.get_config_path("auth"), old_cred)
        check_credential_file(configure.get_config_path("auth"), new_cred)


def test_add_credential_to_nonempty_no_hostname_no_conflict(tmpdir):
    # unfortunately, we must permit duplicate usernames if one has a hostname and the other does not
    old_cred = auth.Auth("username", "password", "example.com")
    new_cred = auth.Auth("username", "other_pass")

    with temp_environ(HOME=str(tmpdir)), \
            patch("hop.auth.load_auth", MagicMock(return_value=[old_cred])), \
            patch("hop.configure.read_new_credential", MagicMock(return_value=new_cred)):
        args = MagicMock()
        args.cred_file = None
        args.force = False
        configure.add_credential(args)
        check_credential_file(configure.get_config_path("auth"), old_cred)
        check_credential_file(configure.get_config_path("auth"), new_cred)


def test_add_credential_to_nonempty_hostname_no_conflict(tmpdir):
    # unfortunately, we must permit duplicate usernames if one has a hostname and the other does not
    old_cred = auth.Auth("username", "password")
    new_cred = auth.Auth("username", "other_pass", "example.com")

    with temp_environ(HOME=str(tmpdir)), \
            patch("hop.auth.load_auth", MagicMock(return_value=[old_cred])), \
            patch("hop.configure.read_new_credential", MagicMock(return_value=new_cred)):
        args = MagicMock()
        args.cred_file = None
        args.force = False
        configure.add_credential(args)
        check_credential_file(configure.get_config_path("auth"), old_cred)
        check_credential_file(configure.get_config_path("auth"), new_cred)


def test_add_credential_conflict_no_host(tmpdir, caplog):
    old_cred = auth.Auth("username", "password")
    new_cred = auth.Auth("username", "other_pass")

    with temp_environ(HOME=str(tmpdir)), \
            patch("hop.configure.read_new_credential", MagicMock(return_value=new_cred)):
        configure.write_auth_data(configure.get_config_path("auth"), [old_cred])
        args = MagicMock()
        args.cred_file = None
        args.force = False
        configure.add_credential(args)
        # without the force option, the old credential should not be overwritten
        check_credential_file(configure.get_config_path("auth"), old_cred)
        assert "Credential already exists; overwrite with --force" in caplog.text

        args.force = True
        configure.add_credential(args)
        # with the force option, the old credential should be overwritten
        check_credential_file(configure.get_config_path("auth"), new_cred)


def test_add_credential_conflict_with_host(tmpdir, caplog):
    old_cred = auth.Auth("username", "password", "example.com")
    new_cred = auth.Auth("username", "other_pass", "example.com")

    with temp_environ(HOME=str(tmpdir)), \
            patch("hop.configure.read_new_credential", MagicMock(return_value=new_cred)):
        configure.write_auth_data(configure.get_config_path("auth"), [old_cred])
        args = MagicMock()
        args.cred_file = None
        args.force = False
        configure.add_credential(args)
        # without the force option, the old credential should not be overwritten
        check_credential_file(configure.get_config_path("auth"), old_cred)
        assert "Credential already exists; overwrite with --force" in caplog.text

        args.force = True
        configure.add_credential(args)
        # with the force option, the old credential should be overwritten
        check_credential_file(configure.get_config_path("auth"), new_cred)


delete_input_creds = [
    auth.Auth("user1", "pass1"),
    auth.Auth("user2", "pass2"),
    auth.Auth("user3", "pass3", "example.com"),
    auth.Auth("user3", "pass3-alt", "example.net"),
]


def check_credential_deletion(config_file, removed_index):
    """Check that a particular credential is not in a config file after removal,
       and that all other are still present.

    Args:
        config_file: Path to the file to check.
        removed_index: Index of the credential in delete_input_creds which should not be present.
    """
    observed = auth.load_auth(config_file)
    for idx, cred in enumerate(delete_input_creds):
        if idx == removed_index:
            assert cred not in observed
        else:
            assert cred in observed


def test_delete_credential_no_match(tmpdir):
    invalid_username = "not-a-user"
    with temp_environ(HOME=str(tmpdir)), \
            patch("hop.auth.load_auth", MagicMock(return_value=delete_input_creds.copy())), \
            pytest.raises(RuntimeError) as err:
        configure.delete_credential(invalid_username)
    assert "No matching credential found" in err.value.args[0]
    assert invalid_username in err.value.args[0]

    invalid_host = "example.com"
    with temp_environ(HOME=str(tmpdir)), \
            patch("hop.auth.load_auth", MagicMock(return_value=delete_input_creds.copy())), \
            pytest.raises(RuntimeError) as err:
        configure.delete_credential(f"{invalid_username}@{invalid_host}")
    assert "No matching credential found" in err.value.args[0]
    assert invalid_username in err.value.args[0]
    assert f" with hostname '{invalid_host}'" in err.value.args[0]


def test_delete_credential_no_match_no_data(tmpdir):
    invalid_username = "not-a-user"
    with temp_environ(HOME=str(tmpdir)), \
            pytest.raises(RuntimeError) as err:
        configure.delete_credential(invalid_username)
    assert "No matching credential found" in err.value.args[0]
    assert invalid_username in err.value.args[0]


def test_delete_credential_no_hostname(tmpdir):
    with temp_environ(HOME=str(tmpdir)):
        with patch("hop.auth.load_auth", MagicMock(return_value=delete_input_creds.copy())):
            configure.delete_credential("user1")
        check_credential_deletion(configure.get_config_path("auth"), 0)


def test_delete_credential_with_hostname(tmpdir):
    with temp_environ(HOME=str(tmpdir)):
        with patch("hop.auth.load_auth", MagicMock(return_value=delete_input_creds.copy())):
            configure.delete_credential("user3@example.com")
        check_credential_deletion(configure.get_config_path("auth"), 2)


def test_delete_credential_ambiguous(tmpdir):
    with temp_environ(HOME=str(tmpdir)), \
            patch("hop.auth.load_auth", MagicMock(return_value=delete_input_creds.copy())), \
            pytest.raises(RuntimeError) as err:
        configure.delete_credential("user3")
    assert "Ambiguous credentials found" in err.value.args[0]
    assert delete_input_creds[2].username in err.value.args[0]
    assert delete_input_creds[2].hostname in err.value.args[0]
    assert delete_input_creds[3].hostname in err.value.args[0]
