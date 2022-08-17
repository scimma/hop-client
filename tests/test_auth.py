from unittest.mock import patch, MagicMock

import pytest

from hop import auth
from hop import configure
import os
import stat
import toml

from conftest import temp_environ, temp_config


def check_credential_file(config_path, cred):
    assert os.path.exists(config_path)
    assert os.stat(config_path).st_mode & 0o7777 == 0o600
    cf = open(config_path, "r")
    config_file_text = cf.read()
    assert cred.username in config_file_text
    assert cred.password in config_file_text
    if len(cred.hostname) > 0:
        assert cred.hostname in config_file_text


def test_auth_username():
    a = auth.Auth("foo", "bar")
    assert a.username == "foo"


def test_auth_password():
    a = auth.Auth("foo", "bar")
    assert a.password == "bar"


def test_auth_hostname():
    a = auth.Auth("foo", "bar")  # use default hostname
    assert a.hostname == ""
    a = auth.Auth("foo", "bar", "example.com")
    assert a.hostname == "example.com"


def test_auth_mechanism():
    a = auth.Auth("foo", "bar")  # use default mechanism
    assert a.mechanism == str(auth.SASLMethod.SCRAM_SHA_512)
    a = auth.Auth("foo", "bar", method=auth.SASLMethod.SCRAM_SHA_256)
    assert a.mechanism == str(auth.SASLMethod.SCRAM_SHA_256)
    a = auth.Auth("foo", "bar", token_endpoint="https://example.com/oauth2/token")
    assert a.mechanism == str(auth.SASLMethod.OAUTHBEARER)
    assert a.token_endpoint == "https://example.com/oauth2/token"


def test_auth_protocol():
    a = auth.Auth("foo", "bar")  # use default security
    assert a.ssl
    assert a.protocol == "SASL_SSL"
    a = auth.Auth("foo", "bar", ssl=False)
    assert not a.ssl
    assert a.protocol == "SASL_PLAINTEXT"


def test_auth_ca_location():
    a = auth.Auth("foo", "bar", ssl=False)
    assert not a.ssl
    assert a.ssl_ca_location is None
    a = auth.Auth("foo", "bar", ssl_ca_location="foo/bar")
    assert a.ssl
    assert a.ssl_ca_location == "foo/bar"


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


def test_load_auth_oidc(auth_config_oidc, tmpdir):
    with temp_config(tmpdir, auth_config_oidc) as config_dir, \
            temp_environ(XDG_CONFIG_HOME=config_dir):
        auth_data = auth.load_auth()
        assert len(auth_data) == 1
        assert auth_data[0].username == "username"
        assert auth_data[0].token_endpoint == "https://example.com/oauth2/token"


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


def test_load_auth_invalid_toml(tmpdir):
    garbage = "KHFBGKJSBVJKbdfb ,s ,msb vks bs"
    with temp_config(tmpdir, garbage) as config_dir, \
            temp_environ(XDG_CONFIG_HOME=config_dir), pytest.raises(RuntimeError):
        auth.load_auth()


def test_load_auth_malformed_legacy(tmpdir):
    missing_username = """
                       [auth]
                       password = "password"
                       extra = "stuff"
                       """
    with temp_config(tmpdir, missing_username) as config_dir, \
            temp_environ(XDG_CONFIG_HOME=config_dir), pytest.raises(RuntimeError):
        auth.load_auth()

    missing_password = """
                       [auth]
                       username = "username"
                       extra = "stuff"
                   """
    with temp_config(tmpdir, missing_password) as config_dir, \
            temp_environ(XDG_CONFIG_HOME=config_dir), pytest.raises(RuntimeError):
        auth.load_auth()


def test_load_auth_malformed(tmpdir):
    missing_username = """
        auth = [{extra="stuff",
            password="password"}]
        """
    with temp_config(tmpdir, missing_username) as config_dir, \
            temp_environ(XDG_CONFIG_HOME=config_dir), pytest.raises(RuntimeError):
        auth.load_auth()

    missing_password = """
    auth = [{username="username",
        extra="stuff"}]
    """
    with temp_config(tmpdir, missing_password) as config_dir, \
            temp_environ(XDG_CONFIG_HOME=config_dir), pytest.raises(RuntimeError):
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
        assert creds[0].password == "pass1"
        assert creds[0].hostname == "host1"
        assert creds[1].username == "user2"
        assert creds[1].password == "pass2"
        assert creds[1].hostname == "host2"


def check_no_auth_data(file_path):
    """Check that a config file contains no auth data, but is otherwise valid TOML

    Returns:
        The configuration which was read from the file
    """
    with open(file_path, "r") as f:
        config_data = toml.loads(f.read())
        assert "auth" not in config_data
        return config_data


def test_prune_outdated_empty(tmpdir):
    with temp_environ(XDG_CONFIG_HOME=str(tmpdir)):
        # when the config file does not exist, this function should successfully do nothing
        auth.prune_outdated_auth()

        config_path = configure.get_config_path()
        os.makedirs(os.path.dirname(config_path), exist_ok=True)
        with open(config_path, "w"):
            pass
        # should also work when the file exists but is empty
        auth.prune_outdated_auth()
        check_no_auth_data(config_path)


def test_prune_outdated_malformed(tmpdir):
    with temp_environ(XDG_CONFIG_HOME=str(tmpdir)):
        config_path = configure.get_config_path()
        os.makedirs(os.path.dirname(config_path), exist_ok=True)
        with open(config_path, "w") as f:
            f.write("not valid TOML IGIUF T J2(YHFOh q3pi8hoU *AHou7w3ht")
        # a RuntimeError should be raised when the file is unparseable garbage
        with pytest.raises(RuntimeError) as err:
            auth.prune_outdated_auth()
        assert f"configuration file {config_path} is malformed" in err.value.args[0]


def test_prune_outdated_auth(tmpdir):
    config_data = """
        foo = "bar"
        auth = [{
        username = "username",
        password = "password",
        hostname = "example.com"
        }]
        baz = "quux"
        """
    with temp_environ(XDG_CONFIG_HOME=str(tmpdir)):
        config_path = configure.get_config_path()
        os.makedirs(os.path.dirname(config_path), exist_ok=True)
        with open(config_path, "w") as f:
            f.write(config_data)
        auth.prune_outdated_auth()
        # the auth data should be gone
        new_config_data = check_no_auth_data(config_path)
        # all other data should remain untouched
        assert "foo" in new_config_data
        assert "baz" in new_config_data

        # the same should work for files in non-default locations
        alt_config_path = f"{os.path.dirname(config_path)}/other.toml"
        with open(alt_config_path, "w") as f:
            f.write(config_data)
        auth.prune_outdated_auth(alt_config_path)
        # the auth data should be gone
        new_config_data = check_no_auth_data(alt_config_path)
        # all other data should remain untouched
        assert "foo" in new_config_data
        assert "baz" in new_config_data


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


def test_read_new_credential_csv(tmpdir):
    csv_file = tmpdir + "/cred.csv"

    # read from a csv file with no hostname
    with open(csv_file, "w") as f:
        f.write("username,password\n")
        f.write("user,pass")
    new_cred = auth.read_new_credential(csv_file)
    assert new_cred.username == "user"
    assert new_cred.password == "pass"
    assert new_cred.hostname == ""

    # read from a csv file with a hostname
    with open(csv_file, "w") as f:
        f.write("username,password,hostname\n")
        f.write("user2,pass2,example.com")
    new_cred = auth.read_new_credential(csv_file)
    assert new_cred.username == "user2"
    assert new_cred.password == "pass2"
    assert new_cred.hostname == "example.com"

    # read from a csv file with a mechanism
    with open(csv_file, "w") as f:
        f.write("username,password,mechanism\n")
        f.write("user3,pass3,SCRAM-SHA-256")
    new_cred = auth.read_new_credential(csv_file)
    assert new_cred.username == "user3"
    assert new_cred.password == "pass3"
    assert new_cred.mechanism == "SCRAM_SHA_256"

    # read from a csv file with a mechanism
    with open(csv_file, "w") as f:
        f.write("username,password,protocol\n")
        f.write("user4,pass4,SASL_PLAINTEXT")
    new_cred = auth.read_new_credential(csv_file)
    assert new_cred.username == "user4"
    assert new_cred.password == "pass4"
    assert not new_cred.ssl
    assert new_cred.protocol == "SASL_PLAINTEXT"

    # read from a csv file with an SSL CA data location
    with open(csv_file, "w") as f:
        f.write("username,password,ssl_ca_location\n")
        f.write("user5,pass5,foo/bar")
    new_cred = auth.read_new_credential(csv_file)
    assert new_cred.username == "user5"
    assert new_cred.password == "pass5"
    assert new_cred.ssl_ca_location == "foo/bar"


def test_read_new_credential_csv_malformed(tmpdir):
    csv_file = tmpdir + "/cred.csv"

    # nonexistent file => FileNotFoundError
    with pytest.raises(FileNotFoundError):
        auth.read_new_credential(csv_file)

    # no username => KeyError
    with open(csv_file, "w") as f:
        f.write("password,hostname\n")
        f.write("pass,example.com")
    with pytest.raises(KeyError):
        auth.read_new_credential(csv_file)

    # no password => KeyError
    with open(csv_file, "w") as f:
        f.write("username,hostname\n")
        f.write("user,example.com")
    with pytest.raises(KeyError):
        auth.read_new_credential(csv_file)


def test_read_new_credential_interactive(tmpdir):
    username = "foo"
    password = "bar"
    for hostname in ["", "example.com"]:
        with patch("getpass.getpass", MagicMock(return_value=password)), \
                patch("hop.auth.input", MagicMock(side_effect=[username, hostname, ""])):
            new_cred = auth.read_new_credential()
            assert new_cred.username == username
            assert new_cred.password == password
            assert new_cred.hostname == hostname


def test_read_new_credential_interactive_invalid(tmpdir):
    username = "foo"
    password = "bar"
    hostname = "example.com"

    # missing username
    with patch("getpass.getpass", MagicMock(return_value=password)), \
            patch("hop.auth.input", MagicMock(side_effect=["", hostname, ""])), \
            pytest.raises(RuntimeError) as err:
        auth.read_new_credential()
    assert err.value.args[0] == "Username may not be empty"

    # missing password
    with patch("getpass.getpass", MagicMock(return_value="")), \
            patch("hop.auth.input", MagicMock(side_effect=[username, hostname, ""])), \
            pytest.raises(RuntimeError) as err:
        auth.read_new_credential()
    assert err.value.args[0] == "Password may not be empty"


def test_hostname_validation():
    username = "foo"
    password = "bar"
    # good cases
    for hostname in ["example.com", "example.com:9092",
                     "kafka://example.com", "kafka://example.com:9092"]:
        processed_hostname = auth._validate_hostname(hostname)
        if "kafka://" in hostname:
            assert processed_hostname == hostname[8:]
        else:
            assert processed_hostname == hostname
    # bad cases
    for hostname in ["http://example.com", "file:///usr/bin", "foo/bar",
                     "kafka://example.com/topic",
                     "kafka://example.com:9092/topic"]:
        with pytest.raises(RuntimeError) as err:
            processed_hostname = auth._validate_hostname(hostname)
        assert "Unable to parse hostname" in err.value.args[0]


def credential_write_read_round_trip(orig_cred, file_path):
    auth.write_auth_data(file_path, [orig_cred])
    read_creds = auth.load_auth(file_path)
    assert len(read_creds) == 1
    assert read_creds[0] == orig_cred


def test_write_config_data(tmpdir):
    config_file = tmpdir + "/config"
    username = "scimma"
    password = "scimmapass"
    auth.write_auth_data(config_file, [auth.Auth(username, password)])
    check_credential_file(config_file, auth.Auth(username, password))

    credential_write_read_round_trip(auth.Auth(username, password), config_file)
    credential_write_read_round_trip(auth.Auth(username, password, host="example.com"), config_file)
    credential_write_read_round_trip(auth.Auth(username, password, ssl=False), config_file)
    credential_write_read_round_trip(auth.Auth(username, password,
                                               method=auth.SASLMethod.SCRAM_SHA_256),
                                     config_file)
    credential_write_read_round_trip(auth.Auth(username, password, ssl_ca_location="ca.cert"),
                                     config_file)


def test_list_credentials(tmpdir, capsys):
    with patch("hop.auth.load_auth", MagicMock(return_value=[])):
        auth.list_credentials()
        captured = capsys.readouterr()
        assert len(captured.out) == 0

    # lie about output going to a TTY to check more user-friendly message used then
    with patch("hop.auth.load_auth", MagicMock(return_value=[])), \
            patch("os.isatty", MagicMock(return_value=True)):
        auth.list_credentials()
        captured = capsys.readouterr()
        assert "No credentials" in captured.out

    short_cred = auth.Auth("user1", "pass1")
    long_cred = auth.Auth("user2", "pass2", "host2")

    with patch("hop.auth.load_auth", MagicMock(return_value=[short_cred])):
        auth.list_credentials()
        captured = capsys.readouterr()
        assert short_cred.username in captured.out
        assert short_cred.password not in captured.out
        assert "for" not in captured.out

    with patch("hop.auth.load_auth", MagicMock(return_value=[short_cred, long_cred])):
        auth.list_credentials()
        captured = capsys.readouterr()
        assert short_cred.username in captured.out
        assert long_cred.username in captured.out
        assert long_cred.password not in captured.out
        assert long_cred.hostname in captured.out


def test_add_credential_to_empty(tmpdir):
    new_cred = auth.Auth("user", "pass")

    with temp_environ(HOME=str(tmpdir)), \
            patch("hop.auth.read_new_credential", MagicMock(return_value=new_cred)):
        args = MagicMock()
        args.cred_file = None
        args.force = False
        auth.add_credential(args)
        check_credential_file(configure.get_config_path("auth"), new_cred)


def test_add_credential_to_nonempty(auth_config, tmpdir):
    old_cred = auth.Auth("username", "password")
    new_cred = auth.Auth("other_user", "other_pass")

    with temp_config(tmpdir, auth_config) as config_dir, temp_environ(HOME=config_dir), \
            patch("hop.auth.read_new_credential", MagicMock(return_value=new_cred)):
        args = MagicMock()
        args.cred_file = None
        args.force = False
        auth.add_credential(args)
        check_credential_file(configure.get_config_path("auth"), old_cred)
        check_credential_file(configure.get_config_path("auth"), new_cred)


def test_add_credential_to_nonempty_no_hostname_no_conflict(tmpdir):
    # unfortunately, we must permit duplicate usernames if one has a hostname and the other does not
    old_cred = auth.Auth("username", "password", "example.com")
    new_cred = auth.Auth("username", "other_pass")

    with temp_environ(HOME=str(tmpdir)), \
            patch("hop.auth.load_auth", MagicMock(return_value=[old_cred])), \
            patch("hop.auth.read_new_credential", MagicMock(return_value=new_cred)):
        args = MagicMock()
        args.cred_file = None
        args.force = False
        auth.add_credential(args)
        check_credential_file(configure.get_config_path("auth"), old_cred)
        check_credential_file(configure.get_config_path("auth"), new_cred)


def test_add_credential_to_nonempty_hostname_no_conflict(tmpdir):
    # unfortunately, we must permit duplicate usernames if one has a hostname and the other does not
    old_cred = auth.Auth("username", "password")
    new_cred = auth.Auth("username", "other_pass", "example.com")

    with temp_environ(HOME=str(tmpdir)), \
            patch("hop.auth.load_auth", MagicMock(return_value=[old_cred])), \
            patch("hop.auth.read_new_credential", MagicMock(return_value=new_cred)):
        args = MagicMock()
        args.cred_file = None
        args.force = False
        auth.add_credential(args)
        check_credential_file(configure.get_config_path("auth"), old_cred)
        check_credential_file(configure.get_config_path("auth"), new_cred)


def test_add_credential_conflict_no_host(tmpdir, caplog):
    old_cred = auth.Auth("username", "password")
    new_cred = auth.Auth("username", "other_pass")

    with temp_environ(HOME=str(tmpdir)), \
            patch("hop.auth.read_new_credential", MagicMock(return_value=new_cred)):
        auth.write_auth_data(configure.get_config_path("auth"), [old_cred])
        args = MagicMock()
        args.cred_file = None
        args.force = False
        auth.add_credential(args)
        # without the force option, the old credential should not be overwritten
        check_credential_file(configure.get_config_path("auth"), old_cred)
        assert "Credential already exists; overwrite with --force" in caplog.text

        args.force = True
        auth.add_credential(args)
        # with the force option, the old credential should be overwritten
        check_credential_file(configure.get_config_path("auth"), new_cred)


def test_add_credential_conflict_with_host(tmpdir, caplog):
    old_cred = auth.Auth("username", "password", "example.com")
    new_cred = auth.Auth("username", "other_pass", "example.com")
    unrelated_cred = auth.Auth("username", "unrelated_pass", "example.org")

    with temp_environ(HOME=str(tmpdir)), \
            patch("hop.auth.read_new_credential", MagicMock(return_value=new_cred)):
        auth.write_auth_data(configure.get_config_path("auth"), [old_cred, unrelated_cred])
        args = MagicMock()
        args.cred_file = None
        args.force = False
        auth.add_credential(args)
        # without the force option, the old credential should not be overwritten
        check_credential_file(configure.get_config_path("auth"), old_cred)
        assert "Credential already exists; overwrite with --force" in caplog.text

        args.force = True
        auth.add_credential(args)
        # with the force option, the old credential should be overwritten
        check_credential_file(configure.get_config_path("auth"), new_cred)
        # also check that unrelated credentials haven't been removed
        creds = auth.load_auth(configure.get_config_path("auth"))
        assert unrelated_cred in creds


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
        auth.delete_credential(invalid_username)
    assert "No matching credential found" in err.value.args[0]
    assert invalid_username in err.value.args[0]

    invalid_host = "example.com"
    with temp_environ(HOME=str(tmpdir)), \
            patch("hop.auth.load_auth", MagicMock(return_value=delete_input_creds.copy())), \
            pytest.raises(RuntimeError) as err:
        auth.delete_credential(f"{invalid_username}@{invalid_host}")
    assert "No matching credential found" in err.value.args[0]
    assert invalid_username in err.value.args[0]
    assert f" with hostname '{invalid_host}'" in err.value.args[0]


def test_delete_credential_no_match_no_data(tmpdir):
    invalid_username = "not-a-user"
    with temp_environ(HOME=str(tmpdir)), \
            pytest.raises(RuntimeError) as err:
        auth.delete_credential(invalid_username)
    assert "No matching credential found" in err.value.args[0]
    assert invalid_username in err.value.args[0]


def test_delete_credential_no_hostname(tmpdir):
    with temp_environ(HOME=str(tmpdir)):
        with patch("hop.auth.load_auth", MagicMock(return_value=delete_input_creds.copy())):
            auth.delete_credential("user1")
        check_credential_deletion(configure.get_config_path("auth"), 0)


def test_delete_credential_with_hostname(tmpdir):
    with temp_environ(HOME=str(tmpdir)):
        with patch("hop.auth.load_auth", MagicMock(return_value=delete_input_creds.copy())):
            auth.delete_credential("user3@example.com")
        check_credential_deletion(configure.get_config_path("auth"), 2)


def test_delete_credential_ambiguous(tmpdir):
    with temp_environ(HOME=str(tmpdir)), \
            patch("hop.auth.load_auth", MagicMock(return_value=delete_input_creds.copy())), \
            pytest.raises(RuntimeError) as err:
        auth.delete_credential("user3")
    assert "Ambiguous credentials found" in err.value.args[0]
    assert delete_input_creds[2].username in err.value.args[0]
    assert delete_input_creds[2].hostname in err.value.args[0]
    assert delete_input_creds[3].hostname in err.value.args[0]


def test_delete_credential_ambiguous_with_host(tmpdir):
    creds = [delete_input_creds[2], delete_input_creds[2]]
    with temp_environ(HOME=str(tmpdir)), \
            patch("hop.auth.load_auth", MagicMock(return_value=creds)), \
            pytest.raises(RuntimeError) as err:
        auth.delete_credential("user3@example.com")
    assert "Ambiguous credentials found" in err.value.args[0]
    assert creds[0].username in err.value.args[0]
    assert creds[0].hostname in err.value.args[0]


def test_delete_credential_ambiguous_creds_without_hosts(tmpdir):
    creds = [auth.Auth("user1", "pass1"), auth.Auth("user1", "pass2")]
    with temp_environ(HOME=str(tmpdir)), \
            patch("hop.auth.load_auth", MagicMock(return_value=creds)), \
            pytest.raises(RuntimeError) as err:
        auth.delete_credential("user1")
    assert "Ambiguous credentials found" in err.value.args[0]
    assert creds[0].username in err.value.args[0]
