from adc import auth
from . import configure
import os
import errno
import toml

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
    ssl : `bool`, optional
        Whether to enable SSL (enabled by default).
    method : `SASLMethod`, optional
        The SASL method to authenticate, default = SASLMethod.SCRAM_SHA_512.
        See valid SASL methods in SASLMethod.
    ssl_ca_location : `str`, optional
        If using SSL via a self-signed cert, a path/location
        to the certificate.
    """

    def __init__(self, user, password, ssl=True, method=SASLMethod.SCRAM_SHA_512, **kwargs):
        super().__init__(user, password, ssl=ssl, method=method, **kwargs)
        self._username = user

    @property
    def username(self):
        return self._username


def load_auth(config_file=None):
    """Configures an Auth instance given a configuration file.

    Args:
        config_file: Path to a configuration file, loading from
            the default location if not given.

    Returns:
        A configured Auth instance.

    Raises:
        KeyError: An error occurred parsing the configuration file.

    """
    if config_file is None:
        config_file = configure.get_config_path()
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
        config = toml.loads(f.read())["auth"]

    # translate config options
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

        if "mechanism" in config:
            mechanism = config["mechanism"].replace("-", "_")
        else:
            mechanism = "SCRAM_SHA_512"

    except KeyError:
        raise KeyError("configuration file is not configured correctly")
    else:
        return Auth(user, password, ssl=ssl, method=SASLMethod[mechanism], **extra_kwargs)
