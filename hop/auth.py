from collections.abc import Mapping
import csv
import errno
import getpass
import logging
import os
import re
import stat
import toml

from adc import auth

from . import configure
from . import cli


SASLMethod = auth.SASLMethod


logger = logging.getLogger("hop")


# thin wrapper over adc's SASLAuth to define
# different defaults used within Hopskotch
class Auth(auth.SASLAuth):
    """Attach SASL-based authentication to a client.

    Returns client-based auth options when called.

    Parameters
    ----------
    user : `str`
        Username to authenticate with.
    password : `str`
        Password to authenticate with.
    host : `str`, optional
        The name of the host for which this authentication is valid.
    ssl : `bool`, optional
        Whether to enable SSL (enabled by default).
    method : `SASLMethod`, optional
        The SASL method to authenticate. The default is SASLMethod.OAUTHBEARER
        if token_endpoint is provided, or SASLMethod.SCRAM_SHA_512 otherwise.
        See valid SASL methods in SASLMethod.
    ssl_ca_location : `str`, optional
        If using SSL via a self-signed cert, a path/location
        to the certificate.
    token_endpoint : `str`, optional
        The OpenID Connect token endpoint URL.
        Required for OAUTHBEARER / OpenID Connect, otherwise ignored.
    """

    def __init__(self, user, password, host="", ssl=True, method=None,
                 token_endpoint=None, **kwargs):
        if method is None and token_endpoint is None:
            method = SASLMethod.SCRAM_SHA_512
        super().__init__(user, password, ssl=ssl, method=method,
                         token_endpoint=token_endpoint, **kwargs)
        self._username = user
        self._hostname = host

    @property
    def username(self):
        """The username for this credential"""
        return self._username

    @property
    def password(self):
        """The password for this credential"""
        return (self._config.get("sasl.password")
                or self._config.get("sasl.oauthbearer.client.secret"))

    @property
    def hostname(self):
        """The hostname with which this creential is associated,
            or the empty string if the credential did not contain this information
        """
        return self._hostname

    @property
    def mechanism(self):
        """The authentication mechanism to use"""
        return self._config["sasl.mechanism"]

    @property
    def protocol(self):
        """The communication protocol to use"""
        return self._config["security.protocol"]

    @property
    def ssl(self):
        """Whether communication should be secured with SSL"""
        return self.protocol != "SASL_PLAINTEXT"

    @property
    def ssl_ca_location(self):
        """The location of the Certfificate Authority data used for SSL,
            or None if SSL is not enabled
        """
        if "ssl.ca.location" not in self._config:
            return None
        return self._config["ssl.ca.location"]

    @property
    def token_endpoint(self):
        """The OpenID Connect token endpoint,
            or None if OpenID Connect is not enabled
        """
        return self._config.get("sasl.oauthbearer.token.endpoint.url")

    def __eq__(self, other):
        return (self._username == other._username
                and self.password == other.password
                and self.hostname == other.hostname
                and self.mechanism == other.mechanism
                and self.protocol == other.protocol
                and self.ssl_ca_location == other.ssl_ca_location
                and self.token_endpoint == other.token_endpoint)


def load_auth(config_file=None):
    """Configures an Auth instance given a configuration file.

    Args:
        config_file: Path to a configuration file, loading from
            the default location if not given.

    Returns:
        A list of configured Auth instances.

    Raises:
        RuntimeError: The config file exists, but has unsafe permissions
                      and will not be read until they are corrected.
        KeyError: An error occurred parsing the configuration file.
        FileNotFoundError: The configuration file, either as specified
                           explicitly or found automatically, does not exist

    """
    if config_file is None:
        config_file = configure.get_config_path("auth")
        # If finding data automatically, as a fallback, look to see if the auth data is in the main
        # config in the old style.
        if not os.path.exists(config_file):
            main_config_file = configure.get_config_path("general")
            if os.path.exists(main_config_file):
                try:
                    return load_auth(main_config_file)
                except RuntimeError:
                    # if the main config file does not contain auth data, rewrite the error into a
                    # complaint that the auth config file does not exist, since that is what should
                    # be fixed
                    raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), config_file)
            # if config.toml does not exist, continue and complain about lack of auth.toml,
            # which is the real issue

    if not os.path.exists(config_file):
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), config_file)
    # check that the config file has suitable permissions
    config_props = os.stat(config_file)
    dangerous_perm_mask = 0o7077
    if (config_props.st_mode & dangerous_perm_mask) != 0:
        raise RuntimeError(f"{config_file} has unsafe permissions: {config_props.st_mode:o}\n"
                           "Please correct it to 0600")

    # load config
    with open(config_file, "r") as f:
        try:
            auth_data = toml.loads(f.read())["auth"]
        except KeyError:
            raise RuntimeError("configuration file has no auth section")
        except Exception as ex:
            raise RuntimeError(f"configuration file is not configured correctly: {ex}")

    return _interpret_auth_data(auth_data)


def prune_outdated_auth(config_file=None):
    """Remove auth data from a general configuration file.

    This can be needed when updating auth data which was read from the general config for backwards
    compatibility, but is then written out to the correct new location in a separate auth config,
    as is now proper. With no further action, this would leave a vestigial copy from before the
    update in the general config file, which would not be rewritten, so this function exists to
    perform the necessary rewrite.

    Args:
        config_file: Path to a configuration file, rewriting
            the default location if not given.

    Raises:
        RuntimeError: The config file is malformed.

    """
    if config_file is None:
        config_file = configure.get_config_path("general")
    if not os.path.exists(config_file):
        return  # nothing to do!
    with open(config_file, "r") as f:
        try:
            config_data = toml.loads(f.read())
        except Exception as ex:
            raise RuntimeError(f"configuration file {config_file} is malformed: {ex}")
    if "auth" in config_data:
        del config_data["auth"]
        # only overwrite if we made a change
        with open(config_file, "w") as f:
            toml.dump(config_data, f)


def _interpret_auth_data(auth_data):
    """Configures Auth instances given data read from a configuration file.

    Args:
        auth_data: The data obtained from the top-level 'auth' TOML key

    Returns:
        A list of configured Auth instances.

    Raises:
        KeyError: A required key is missing from a credential entry.

    """
    if isinstance(auth_data, Mapping):
        # upgrade old-style single dict configs to new-style list-of-dicts (with one item)
        auth_data = [auth_data]

    auth = []
    for config in auth_data:
        # translate config options
        host = ""
        ssl = True
        extra_kwargs = {}
        try:
            # SSL options
            if "protocol" in config and config["protocol"] == "SASL_PLAINTEXT":
                ssl = False
            elif "ssl_ca_location" in config:
                extra_kwargs["ssl_ca_location"] = config["ssl_ca_location"]

            # SASL options
            user = config["username"]
            password = config["password"]

            if "hostname" in config:
                host = config["hostname"]

            token_endpoint = config.get("token_endpoint")

            if "mechanism" in config:
                mechanism = config["mechanism"].replace("-", "_")
            elif token_endpoint:
                mechanism = "OAUTHBEARER"
            else:
                mechanism = "SCRAM_SHA_512"

        except KeyError as ke:
            raise RuntimeError("configuration file is not configured correctly: "
                               f"missing auth property {ke}")
        else:
            auth.append(Auth(user, password, host=host, ssl=ssl, method=SASLMethod[mechanism],
                             token_endpoint=token_endpoint, **extra_kwargs))
    return auth


def select_matching_auth(creds, hostname, username=None):
    """Selects the most appropriate credential to use when attempting to contact the given host.

    Args:
        creds: A list of configured Auth objects. These can be obtained from :meth:`load_auth`.
        hostname: The name of the host for which to select suitable credentials.
        username: `str`, optional
            The name of the credential to use.

    Returns:
        A single Auth object which should be used to authenticate.

    Raises:
        RuntimeError: Too many or too few credentials matched.
    """

    matches = []
    exact_hostname_match = False
    inexact_hostname_match = False

    for cred in creds:
        maybe_exact_hostname_match = False
        maybe_inexact_hostname_match = False
        # If the credential has a hostname, we require it to match.
        # If it doesn't, we have to assume it might suit the user's purposes.
        if len(cred.hostname) > 0:
            if cred.hostname != hostname:
                continue
            else:
                # the hostname does match, but this credential might be rejected for other reasons
                # so we cannot directly update the global exact_hostname_match
                maybe_exact_hostname_match = True
        else:
            maybe_inexact_hostname_match = True

        # If the user specified a specific username, we require that to be matched.
        if username is not None and cred.username != username:
            continue

        matches.append(cred)
        exact_hostname_match |= maybe_exact_hostname_match
        inexact_hostname_match |= maybe_inexact_hostname_match

    # if there were both exact and inexact matches, cull the inexact matches
    if inexact_hostname_match and exact_hostname_match:
        matches = [match for match in matches if match.hostname == hostname]

    if len(matches) == 0:
        err = f"No matching credential found for hostname '{hostname}'"
        if username is not None:
            err += f" with username '{username}'"
        raise RuntimeError(err)

    if len(matches) > 1:
        err = f"Ambiguous credentials found for hostname '{hostname}'"
        err += f" with username '{username}'" if username is not None \
            else " with no username specified"
        err += "Matched credentials:"
        for match in matches:
            err += f"\n  {match.username}"
            err += " which has no associated hostname" if len(match.hostname) == 0 \
                else f" for {match.hostname}"
        raise RuntimeError(err)

    # At this point we should have exactly one match
    return matches[0]


def _validate_hostname(input: str):
    """Ensure that a value entered for a hostname looks plausible.

    The user is supposed to enter a hostname, possibly with a port, not a URL.
    However, since users are likely to enter something valid prefixed with a `kafka://` scheme,
    accept that with a warning.
    We do not use standard URL parsing since the input is not supposed to be a URL, and attempting
    to parse it as such when no scheme is present mostly just leads to ambiguous results.

    Args:
        input: A string entered by the user when prompted for a hostname.

    Returns:
        The sanitized hostname.

    Raises:
        RuntimeError: If the user input is not acceptable.
    """
    name_re = re.compile("^(kafka://)?([^:/]*(:[0-9]*)?)$")
    match = name_re.match(input)
    if match is None:
        raise RuntimeError("Unable to parse hostname. "
                           "Please enter either `hostname` or `hostname:port`.")
    if match.group(1) is not None:
        logger.warning(f"Ignoring '{match.group(1)}' prefix on hostname")
    return match.group(2)


def read_new_credential(csv_file=None):
    """Import a credential from a CSV file or obtain it interactively from the user.

    Args:
        csv_file: Path to a file from which to read credential data in CSV format.
                  If unspecified, the user will be prompted to enter data instead.

    Returns:
        A configured `Auth` object containing the new credential.

    Raises:
        FileNotFoundError: If csv_file is not None and refers to a nonexistent path.
        KeyError: If csv_file is not None and the specified file does not contain either a username
                  or password field.
        RuntimeError: If csv_file is None and the interactively entered username or passwod is
                      empty.

    """
    options = {}
    if csv_file is None:
        logger.info("Generating configuration with user-specified username + password")
        username = input("Username: ")
        if len(username) == 0:
            raise RuntimeError("Username may not be empty")
        password = getpass.getpass()
        if len(password) == 0:
            raise RuntimeError("Password may not be empty")
        hostname = _validate_hostname(input("Hostname (may be empty): "))
        token_endpoint = input("Token endpoint (empty if not applicable): ") or None
    else:
        if os.path.exists(csv_file):
            with open(csv_file, "r") as f:
                reader = csv.DictReader(f)
                cred = next(reader)
                username = cred["username"]
                password = cred["password"]
                hostname = cred["hostname"] if "hostname" in cred else ""
                token_endpoint = cred.get("token_endpoint")
                if "mechanism" in cred:
                    options["method"] = cred["mechanism"].replace("-", "_")
                if "protocol" in cred:
                    options["ssl"] = cred["protocol"] != "SASL_PLAINTEXT"
                if "ssl_ca_location" in cred:
                    options["ssl_ca_location"] = cred["ssl_ca_location"]
        else:
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), csv_file)
    return Auth(username, password, hostname, token_endpoint=token_endpoint, **options)


def write_auth_data(config_file, credentials):
    """Write configuration file for the set of credentials.

    Creates containing directories as needed.

    Args:
        config_file: configuration file path
        credentials: list of `Auth` objects representing credentials to be stored

    """
    cred_list = []
    for cred in credentials:
        cred_dict = {"username": cred.username, "password": cred.password,
                     "protocol": cred.protocol, "mechanism": cred.mechanism,
                     "token_endpoint": cred.token_endpoint}
        if len(cred.hostname) > 0:
            cred_dict["hostname"] = cred.hostname
        if cred.ssl_ca_location is not None:
            cred_dict["ssl_ca_location"] = cred.ssl_ca_location

        cred_list.append(cred_dict)

    os.makedirs(os.path.dirname(config_file), exist_ok=True)
    fd = os.open(config_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, stat.S_IRUSR | stat.S_IWUSR)
    with open(fd, "w") as f:
        toml.dump({"auth": cred_list}, f)
    logger.info(f"Wrote configuration to: {config_file}")


def list_credentials():
    """Display a list of all configured credentials.

    """
    creds = load_auth()
    max_username_len = max([len(c.username) for c in creds]) if len(creds) > 0 else 1
    long_format = f"{{:{max_username_len}}} for {{}}"
    for cred in creds:
        if len(cred.hostname) > 0:
            print(str.format(long_format, cred.username, cred.hostname))
        else:
            print(cred.username)
    if len(creds) == 0 and os.isatty(1):
        print("No credentials configured")


def add_credential(args):
    """Load a new credential and store it to the persistent configuration.

    Args:
        args: Command line options/arguments object.
              args.cred_file is taken as the path to a CSV file to import, or if None the user is
              prompted to enter a credential directly.
              args.force controls whether an existing credential with an identical name will be
              overwritten.
    """
    # first load any existing credentials
    try:
        creds = load_auth()
    except FileNotFoundError:
        # if no auth file exists we can just treat that as there being no credentials
        creds = []

    # next, load the new credential
    new_cred = read_new_credential(args.cred_file)

    # check for any conflicts between the new credential and an existing one
    conflicting_cred_idx = None
    for idx, cred in enumerate(creds):
        if cred.username == new_cred.username:
            if len(cred.hostname) > 0 and len(new_cred.hostname) > 0 \
                    and cred.hostname == new_cred.hostname:
                conflicting_cred_idx = idx
            elif len(cred.hostname) == 0 and len(new_cred.hostname) == 0:
                conflicting_cred_idx = idx
    if conflicting_cred_idx is not None:
        if args.force:
            creds[conflicting_cred_idx] = new_cred
        else:
            logger.error("Credential already exists; overwrite with --force")
            return
    else:
        creds.append(new_cred)
    write_auth_data(configure.get_config_path("auth"), creds)
    prune_outdated_auth()


def _construct_ambiguous_deletion_message(username, hostname, matches):
    """Create an error message for an ambiguous request to delete a credential.

    This function should only be used by `delete_credential`.

    Args:
        username: The username for the credential targeted for deletion.
        hostname: The hostname for the credential targeted for deletion, if any.
        matches: the collection of possibly matching credentials found by delete_credential.

    Returns:
        An error message string.

    """
    err = f"Ambiguous credentials found for username '{username}'"
    if hostname is not None:
        err += f" with hostname '{hostname}'\n"
    else:
        err += " with no hostname specified\n"
    err += "Matched credentials:"
    for match in matches:
        err += f"\n  {match.username}"
        if len(match.hostname) == 0:
            err += " which has no associated hostname"
        else:
            err += f" for {match.hostname}"
    return err


def delete_credential(name: str):
    """Delete a credential from the persistent configuration.

    Args:
        name: The username, or username and hostname separated by an '@' character of the credential
        to delete.

    Raises:
        RuntimeError: If no credentials or more than one credential matches the specified name,
                      making the operation impossible or ambiguous.
    """
    # first load any existing credentials
    try:
        creds = load_auth()
    except FileNotFoundError:
        # if no auth file exists we can just treat that as there being no credentials
        creds = []

    if '@' in name:
        username, hostname = name.split('@')
    else:
        username = name
        hostname = None

    # next, try to figure out which one we're supposed to remove
    matches = []
    match_indices = []

    for idx, cred in enumerate(creds):
        # the username must match
        if cred.username != username:
            continue
        # if specified, the hostname must match
        if hostname is not None and cred.hostname != hostname:
            continue

        matches.append(cred)
        match_indices.append(idx)

    if len(matches) == 0:
        err = f"No matching credential found with username '{username}'"
        if hostname is not None:
            err += f" with hostname '{hostname}'"
        raise RuntimeError(err)
    elif len(matches) > 1:
        raise RuntimeError(_construct_ambiguous_deletion_message(username, hostname, matches))

    # At this point we should have exactly one match, which we can delete
    del creds[match_indices[0]]
    write_auth_data(configure.get_config_path("auth"), creds)
    prune_outdated_auth()


def _add_parser_args(parser):
    cli.add_logging_opts(parser)

    subparser = parser.add_subparsers(title="commands", metavar="<command>", dest="command")
    subparser.required = True

    subparser.add_parser("locate", help="display configuration path")

    subparser.add_parser("list", help="Display all stored credentials")

    add_cred_parser = subparser.add_parser("add", help="Load a credential, "
                                           "specified either via a CSV file or interactively")
    add_cred_parser.add_argument("cred_file", type=str, default=None, nargs='?',
                                 help="Import credentials from CSV file")
    add_cred_parser.add_argument("--force", action="store_true", default=False,
                                 help="If set, overwrite any existing credential when a new one "
                                 "with the same username and hostname is added")

    del_cred_parser = subparser.add_parser("remove", help="Delete a stored credential")
    del_cred_parser.add_argument("name", type=str,
                                 help="The name of the credential to delete. This will be treated "
                                 "as a username, unless it contains a @ character, in which case it"
                                 " will be split into a username and hostname.")


def _main(args):
    """Authentication configuration utilities.

    """
    cli.set_up_logger(args)

    if args.command == "locate":
        print(configure.get_config_path("auth"))
    elif args.command == "list":
        list_credentials()
    elif args.command == "add":
        add_credential(args)
    elif args.command == "remove":
        delete_credential(args.name)
