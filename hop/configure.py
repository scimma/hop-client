import getpass
import logging
import os
import csv
import errno
import stat

import toml

from . import auth

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


def _add_parser_args(parser):
    subparser = parser.add_subparsers(title="commands", metavar="<command>", dest="command")
    subparser.required = True

    locate_parser = subparser.add_parser("locate", help="display configuration path")
    locate_parser.add_argument("-t", "--type", dest="type", default="general", required=False,
                               choices=["general", "auth"],
                               help="The type of configuration file to locate. "
                               "Defaults to general configuration.")

    setup_subparser = subparser.add_parser("setup", help="set up configuration")
    setup_subparser.add_argument(
        "-f", "--force", action="store_true", help="If set, overrides current configuration",
    )

    setup_subparser.add_argument(
        "-i", "--import", dest="import_cred", help="Import credentials from CSV file",
    )

    subparser.add_parser("list-creds", help="Display all stored credentials")

    add_cred_parser = subparser.add_parser("add-cred", help="Store an additional credential, "
                                           "specified either via a CSV file or interactively")
    add_cred_parser.add_argument("cred_file", type=str, default=None, nargs='?',
                                 help="Import credentials from CSV file")

    del_cred_parser = subparser.add_parser("delete-cred", help="Delete a stored credential")
    del_cred_parser.add_argument("name", type=str,
                                 help="The name of the credential to delete. This will be treated "
                                 "as a username, unless it contains a @ character, in which case it"
                                 " will be split into a username and hostname.")


def read_new_credential(csv_file=None):
    """Import a credential from a CSV file or obtain it interactively from the user.

    Args:
        csv_file: Path to a file from which to read credential data in CSV format.
                  If unspecified, the user will be prompted to enter data instead.

    Returns:
        A configured `auth.Auth` object containing the new credential.

    Raises:
        FileNotFoundError: If csv_file is not None and refers to a nonexistent path.
        KeyError: If csv_file is not None and the specified file does not contain either a username
                  or password field.
        RuntimeError: If csv_file is None and the interactively entered username or passwod is
                      empty.

    """
    if csv_file is None:
        logger.info("Generating configuration with user-specified username + password")
        username = input("Username: ")
        if len(username) == 0:
            raise RuntimeError("Username may not be empty")
        password = getpass.getpass()
        if len(password) == 0:
            raise RuntimeError("Password may not be empty")
        hostname = input("Hostname (may be empty): ")
    else:
        if os.path.exists(csv_file):
            with open(csv_file, "r") as f:
                reader = csv.DictReader(f)
                cred = next(reader)
                username = cred["username"]
                password = cred["password"]
                hostname = cred["hostname"] if "hostname" in cred else ""
        else:
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), csv_file)
    return auth.Auth(username, password, hostname)


def write_auth_data(config_file, credentials):
    """Write configuration file for the set of credentials.

    Creates containing directories as needed.

    Args:
        config_file: configuration file path
        credentials: list of `auth.Auth` objects representing credentials to be stored

    """
    cred_list = []
    for cred in credentials:
        cred_dict = {"username": cred.username, "password": cred._config["sasl.password"]}
        if len(cred.hostname) > 0:
            cred_dict["hostname"] = cred.hostname
        cred_list.append(cred_dict)

    os.makedirs(os.path.dirname(config_file), exist_ok=True)
    fd = os.open(config_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, stat.S_IRUSR | stat.S_IWUSR)
    with open(fd, "w") as f:
        toml.dump({"auth": cred_list}, f)
        logger.info(f"Wrote configuration to: {config_file}")


def set_up_configuration(config_file, csv_file):
    """Set up configuration file.

    Args:
        config_file: Configuration file path
        csv_file: Path to csv credentials file

    """
    write_auth_data(config_file, [read_new_credential(csv_file)])


def list_credentials():
    """Display a list of all configured credentials.

    """
    creds = auth.load_auth()
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
        creds = auth.load_auth()
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
            creds[idx] = new_cred
        else:
            logger.error("Credential already exists; overwrite with --force")
            return
    else:
        creds.append(new_cred)
    write_auth_data(get_config_path("auth"), creds)
    auth.prune_outdated_auth()


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
        creds = auth.load_auth()
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
        err = f"Ambiguous credentials found for username '{username}'"
        err += f" with hostname '{hostname}'" if hostname is not None \
            else " with no hostname specified"
        err += "Matched credentials:"
        for match in matches:
            err += f"\n  {match.username}"
            err += " which has no associated hostname" if len(match.hostname) == 0 \
                else f" for {match.hostname}"
        raise RuntimeError(err)

    # At this point we should have exactly one match, which we can delete
    del creds[match_indices[0]]
    write_auth_data(get_config_path("auth"), creds)
    auth.prune_outdated_auth()


def _main(args):
    """Configuration utilities.

    """
    config_file = get_config_path("auth")
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s | %(name)s : %(levelname)s : %(message)s",
    )

    if args.command == "locate":
        print(config_file)
    elif args.command == "setup":
        if os.path.exists(config_file) and not args.force:
            logger.warning("Configuration already exists, overwrite file with --force")
        else:
            set_up_configuration(config_file, args.import_cred)
    elif args.command == "list-creds":
        list_credentials()
    elif args.command == "add-cred":
        add_credential(args)
    elif args.command == "delete-cred":
        delete_credential(args.name)
