from adc import auth
from . import configure
import os
import errno
import toml
from collections import Mapping

SASLMethod = auth.SASLMethod


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
        The SASL method to authenticate, default = SASLMethod.SCRAM_SHA_512.
        See valid SASL methods in SASLMethod.
    ssl_ca_location : `str`, optional
        If using SSL via a self-signed cert, a path/location
        to the certificate.
    """

    def __init__(self, user, password, host="", ssl=True, method=SASLMethod.SCRAM_SHA_512,
                 **kwargs):
        super().__init__(user, password, ssl=ssl, method=method, **kwargs)
        self._username = user
        self._hostname = host

    @property
    def username(self):
        return self._username

    @property
    def hostname(self):
        return self._hostname


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
                except KeyError:
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
        auth_data = toml.loads(f.read())["auth"]

    return _interpret_auth_data(auth_data)


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

            if "mechanism" in config:
                mechanism = config["mechanism"].replace("-", "_")
            else:
                mechanism = "SCRAM_SHA_512"

        except KeyError:
            raise KeyError("configuration file is not configured correctly")
        else:
            auth.append(Auth(user, password, host=host, ssl=ssl, method=SASLMethod[mechanism],
                             **extra_kwargs))
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
