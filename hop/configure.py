import dataclasses
import logging
import os
import toml
from typing import Optional
import warnings

from . import cli


logger = logging.getLogger("hop")


def get_config_path(type: str = "general"):
    """Determines the default location for auth configuration.

    Args:
        type: The type of configuration data for which the path should be looked up.
              Recognized types are 'general' and 'auth'.

    Returns:
        The path to the requested configuration file.

    Raises:
        ValueError: Unrecognized config type requested.

    """

    file_for_type = {
        "general": "config.toml",
        "auth": "auth.toml",
    }

    if type not in file_for_type:
        raise ValueError(f"Unknown configuration type: '{type}'")

    auth_filepath = os.path.join("hop", file_for_type[type])
    if "XDG_CONFIG_HOME" in os.environ:
        return os.path.join(os.getenv("XDG_CONFIG_HOME"), auth_filepath)
    else:
        return os.path.join(os.getenv("HOME"), ".config", auth_filepath)


@dataclasses.dataclass
class Config:
    """A container class for general configuration parameters
    """
    fetch_external: bool = True
    automatic_offload: bool = True

    def asdict(self):
        return dataclasses.asdict(self)

    @classmethod
    def load(cls, input):
        if hasattr(input, "read"):
            input = input.read()
        try:
            return cls(**toml.loads(input)["config"])
        except KeyError:
            raise RuntimeError("configuration data has no config section") from None
        except Exception as ex:
            raise RuntimeError(f"configuration data is malformed: {ex}")

    def save(self, output):
        toml.dump({"config": self.asdict()}, output)


def _parse_bool(s: str):
    """Interpret a range of possible truthy string values as booleans
    """
    s = s.lower()
    if s == "true" or s == "yes" or s == "on" or s == "1":
        return True
    if s == "false" or s == "no" or s == "off" or s == "0":
        return False
    raise ValueError(f"Invalid boolean value '{s}'")


# The mapping of types to functions used for parsing strings provided by the user into configuration
# values. When no entry for a given type exists in this object, the type's normal constructor should
# be used.
_conversion_functions = {
    bool: _parse_bool
}


def load_config(config_path: Optional[str] = None):
    """
    Load the configuration settings for the library combining, in order of increasing precedence:
    1. The built-in defaults
    2. Defaults stored in the configuration file
    3. Values currently set in the environment as environment variables

    Args:
        config_path: The path to the configuration file to load. If unspecified, the default
                     location is used.
    Return: A Config object with all current settings
    """
    # First, load the specified file, or the default file if not specified
    # If the file does not exist, use built-in defaults
    if config_path is None:
        config_path = get_config_path("general")
    if os.path.exists(config_path):
        try:
            file = open(config_path, 'r')
        except Exception:
            warnings.warn(f"Unable to open {config_path} for reading; using default configuration")
            config = Config()
        else:
            try:
                with file:
                    config = Config.load(file)
            except Exception as ex:
                raise RuntimeError(f"Error loading {config_path}") from ex
    else:
        config = Config()

    # Second, check for any environment variables and apply them if found
    env_var_prefix = "HOP_"

    for field in dataclasses.fields(config):
        var_name = (env_var_prefix + field.name).upper()
        value = os.getenv(var_name)
        if value is None:
            continue
        try:
            if field.type in _conversion_functions:
                value = _conversion_functions[field.type](value)
            else:
                value = field.type(value)
        except Exception as ex:
            raise RuntimeError(f"Error parsing configuration variable {var_name}") from ex
        setattr(config, field.name, value)

    return config


def write_config_value(name: str, value):
    """
    Store a configuration value to the configuation file.

    Args:
        name: The name of the parameter to store. Must be a valid parameter name.
        value: The parameter value to store. Must be the correct type for the specified parameter.
    """
    config_path = get_config_path("general")

    # Load any existing configuration
    # We do not use load_config because we want to avoid baking in any temporary settings
    # provided in environment variables, or any built-in defaults not explicitly chosen by the
    # user.
    if os.path.exists(config_path):
        try:
            config = toml.load(config_path)["config"]
        except KeyError:
            raise RuntimeError("{config_path} has no config section") from None
        except Exception as ex:
            raise RuntimeError(f"{config_path} is malformed: {ex}")
    else:
        config = {}

    # Set/overwrite the parameter specified by the user
    config[name] = value

    # Write the resulting configuration back out
    os.makedirs(os.path.dirname(config_path), exist_ok=True)
    with open(config_path, 'w') as output:
        toml.dump({"config": config}, output)

    logger.info(f"Wrote {name} = {value} to {config_path}")


def _add_parser_args(parser):
    cli.add_logging_opts(parser)

    subparser = parser.add_subparsers(title="commands", metavar="<command>", dest="command")
    subparser.required = True

    locate_parser = subparser.add_parser("locate", help="display configuration path")
    locate_parser.add_argument("-t", "--type", dest="type", default="general", required=False,
                               choices=["general", "auth"],
                               help="The type of configuration file to locate. "
                               "Defaults to general configuration.")

    subparser.add_parser("show", help="display current configuration")

    set_parser = subparser.add_parser("set", help="set default value for a configuration parameter")
    set_parser.add_argument("name",
                            choices=[field.name for field in dataclasses.fields(Config)],
                            help="The configuration parameter to set")
    set_parser.add_argument("value",
                            help="The parameter value to set")


def _main(args):
    """Configuration utilities.

    """
    cli.set_up_logger(args)

    if args.command == "locate":
        print(get_config_path(args.type))
    elif args.command == "show":
        config = load_config()
        for field in dataclasses.fields(config):
            print(f"{field.name}: {getattr(config, field.name)}")
    elif args.command == "set":
        # Collect data supplied by the user
        name = args.name
        # the argument parser should enforce that name is a value configuration parameter
        for field in dataclasses.fields(Config):
            if field.name == name:
                break
        value = args.value
        # sanitize/normalize the provided value
        if field.type in _conversion_functions:
            value = _conversion_functions[field.type](value)
        else:
            value = field.type(value)

        write_config_value(name, value)
