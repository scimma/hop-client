from unittest.mock import patch

import pytest

from hop import auth
from hop import configure
import os
import stat

from conftest import temp_environ, temp_config


def test_load_auth_legacy(legacy_auth_config, tmpdir):
    with temp_config(tmpdir, legacy_auth_config) as config_dir, \
            temp_environ(XDG_CONFIG_HOME=config_dir):
        auth_data = auth.load_auth()
        assert len(auth_data) == 1
        assert auth_data[0].username == "username"


def test_load_auth(auth_config, tmpdir):
    with temp_config(tmpdir, auth_config) as config_dir, temp_environ(XDG_CONFIG_HOME=config_dir):
        auth_data = auth.load_auth()
        assert len(auth_data) == 1
        assert auth_data[0].username == "username"


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


def test_load_auth_malformed_legacy(tmpdir):
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


def test_load_auth_malformed(tmpdir):
    missing_username = """
        auth = [{extra="stuff",
            password="password"}]
        """
    with temp_config(tmpdir, missing_username) as config_dir, \
            temp_environ(XDG_CONFIG_HOME=config_dir), pytest.raises(KeyError):
        auth.load_auth()

    missing_password = """
    auth = [{username="username",
        extra="stuff"}]
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
    use_plaintext = """auth = [{
                       username = "username",
                       password = "password",
                       protocol = "SASL_PLAINTEXT"
                       }]"""
    with temp_config(tmpdir, use_plaintext) as config_dir, \
            temp_environ(XDG_CONFIG_HOME=config_dir), patch("hop.auth.Auth") as auth_mock:
        auth.load_auth()
        assert auth_mock.called_with(ssl=False)

    # An SSL CA data path should be honored
    with_ca_data = """auth = [{
                   username = "username",
                   password = "password",
                   ssl_ca_location = "/foo/bar/baz"
                   }]"""
    with temp_config(tmpdir, with_ca_data) as config_dir, \
            temp_environ(XDG_CONFIG_HOME=config_dir), patch("hop.auth.Auth") as auth_mock:
        auth.load_auth()
        assert auth_mock.called_with(ssl_ca_location="/foo/bar/baz")

    # Alternate mechanisms should be honored
    plain_mechanism = """auth = [{
                      username = "username",
                      password = "password",
                      mechanism = "PLAIN"
                      }]"""
    with temp_config(tmpdir, plain_mechanism) as config_dir, \
            temp_environ(XDG_CONFIG_HOME=config_dir), patch("hop.auth.Auth") as auth_mock:
        auth.load_auth()
        assert auth_mock.called_with(mechanism=SASLMethod.PLAIN)

    # Associated hostnames should be included
    with_host = """auth = [{
                username = "username",
                password = "password",
                hostname = "example.com"
                }]"""
    with temp_config(tmpdir, with_host) as config_dir, \
            temp_environ(XDG_CONFIG_HOME=config_dir):
        creds = auth.load_auth()
        assert len(creds) == 1
        assert creds[0].hostname == "example.com"


def test_load_auth_muliple_creds(tmpdir):
    two_creds = """auth = [
                    {
                        username = "user1",
                        password = "pass1",
                        hostname = "host1"
                    },
                    {
                        username = "user2",
                        password = "pass2",
                        hostname = "host2"
                    },
                ]"""
    with temp_config(tmpdir, two_creds) as config_dir, temp_environ(XDG_CONFIG_HOME=config_dir):
        creds = auth.load_auth()
        assert len(creds) == 2
        assert creds[0].username == "user1"
        assert creds[0]._config["sasl.password"] == "pass1"
        assert creds[0].hostname == "host1"
        assert creds[1].username == "user2"
        assert creds[1]._config["sasl.password"] == "pass2"
        assert creds[1].hostname == "host2"


def test_select_auth_no_match(auth_config, tmpdir):
    no_match = "No matching credential found"

    # no credentials at all
    with pytest.raises(RuntimeError) as err:
        selected = auth.select_matching_auth([], "example.com", "nosuchuser")
    assert "No matching credential found for hostname 'example.com'" in err.value.args[0]

    # no match for requested hostname
    with_host = """auth = [{
        username = "username",
        password = "password",
        hostname = "example.com"
        }]"""
    with temp_config(tmpdir, with_host) as config_dir, \
            temp_environ(XDG_CONFIG_HOME=config_dir), pytest.raises(RuntimeError) as err:
        creds = auth.load_auth()
        selected = auth.select_matching_auth(creds, "example.net")
    assert f"{no_match} for hostname 'example.net'" in err.value.args[0]

    # no match for requested username
    with temp_config(tmpdir, auth_config) as config_dir, \
            temp_environ(XDG_CONFIG_HOME=config_dir), pytest.raises(RuntimeError) as err:
        creds = auth.load_auth()
        selected = auth.select_matching_auth(creds, "example.com", "nosuchuser")
    assert f"{no_match} for hostname 'example.com' with username 'nosuchuser'" in err.value.args[0]


def test_select_auth_ambiguity():
    too_many = "Ambiguous credentials found"

    two_vague_creds = [
        auth.Auth("user1", "pass1"),
        auth.Auth("user2", "pass2"),
    ]

    # given two credetials with no associated hostnames, any lookup which does not specify the
    # username will be ambiguous
    with pytest.raises(RuntimeError) as err:
        selected = auth.select_matching_auth(two_vague_creds, "example.com")
    assert f"{too_many} for hostname 'example.com' with no username specified" in err.value.args[0]
    assert "user1 which has no associated hostname" in err.value.args[0]
    assert "user2 which has no associated hostname" in err.value.args[0]

    # specifying a valid username should resolve the ambiguity
    for username in ["user1", "user2"]:
        selected = auth.select_matching_auth(two_vague_creds, "example.com", username)
        assert selected.username == username

    two_vague_creds = [
        auth.Auth("user1", "pass1"),
        auth.Auth("user3", "pass3", host="example.com"),
    ]
    # given a credential with no hostname and one with, a request which matches the one with a
    # hostname should _not_ be ambiguous
    selected = auth.select_matching_auth(two_vague_creds, "example.com")
    assert selected.username == "user3"

    two_specific_creds = [
        auth.Auth("user3", "pass3", host="example.com"),
        auth.Auth("user4", "pass4", host="example.com"),
    ]

    # given two credentials which both exactly match the requested hostname there should again be
    # an ambiguity
    with pytest.raises(RuntimeError) as err:
        selected = auth.select_matching_auth(two_specific_creds, "example.com")
    assert f"{too_many} for hostname 'example.com'" in err.value.args[0]
    assert "user3 for example.com" in err.value.args[0]
    assert "user4 for example.com" in err.value.args[0]

    # specifying a valid username should again resolve the ambiguity
    for username in ["user3", "user4"]:
        selected = auth.select_matching_auth(two_specific_creds, "example.com", username)
        assert selected.username == username

    # No single source of credentials should issue two credentials with the same username, however,
    # two separate issuers could issue ones with the same name, and if neither specifies a hostname
    # they will be ambiguous.
    # (Duplicate usernames with matching hostnames should be impossible, as there should not be two
    # issuers for the same hostname)
    duplicate_users = [
        auth.Auth("user", "pass5"),
        auth.Auth("user", "pass6"),
    ]
    with pytest.raises(RuntimeError) as err:
        selected = auth.select_matching_auth(duplicate_users, "example.com")
    assert f"{too_many} for hostname 'example.com'" in err.value.args[0]
    assert "user which has no associated hostname" in err.value.args[0]
    assert "user which has no associated hostname" in err.value.args[0]

    # specifying a username woun't help, since they are duplicates
    with pytest.raises(RuntimeError) as err:
        selected = auth.select_matching_auth(duplicate_users, "example.com", "user")
    assert f"{too_many} for hostname 'example.com'" in err.value.args[0]
    assert "user which has no associated hostname" in err.value.args[0]
    assert "user which has no associated hostname" in err.value.args[0]


def test_auth_location_fallback(tmpdir):
    valid_auth = "auth = [{username=\"user\",password=\"pass\"}]"
    other_auth = "auth = [{username=\"other-user\",password=\"other-pass\"}]"

    config_dir = f"{tmpdir}/hop"
    os.makedirs(config_dir, exist_ok=True)

    def write_file(name: str, data: str):
        file_path = f"{config_dir}/{name}"
        with open(file_path, 'w') as file:
            os.chmod(file_path, stat.S_IRUSR | stat.S_IWUSR)
            file.write(data)

    with temp_environ(XDG_CONFIG_HOME=str(tmpdir)):
        # If both auth.toml and config.toml contain auth data, only auth.toml should be read
        write_file("auth.toml", valid_auth)
        write_file("config.toml", other_auth)
        creds = auth.load_auth()
        assert len(creds) == 1
        assert creds[0].username == "user"

        # If auth.toml does not exist and config.toml contains valid auth data, it should be read
        os.remove(f"{config_dir}/auth.toml")
        creds = auth.load_auth()
        assert len(creds) == 1
        assert creds[0].username == "other-user"

        # If auth.toml does not exist and config.toml exists but contains no valid auth data, the
        # resulting error should be about auth.toml
        write_file("config.toml", "")
        with pytest.raises(FileNotFoundError) as err:
            creds = auth.load_auth()
        assert "auth.toml" in err.value.filename

        # If neither auth.toml nor config.toml exixts, the error should be about auth.toml
        os.remove(f"{config_dir}/config.toml")
        with pytest.raises(FileNotFoundError) as err:
            creds = auth.load_auth()
        assert "auth.toml" in err.value.filename


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
        configuration_file = configure.get_config_path("auth")
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
