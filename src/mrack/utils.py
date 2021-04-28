# Copyright 2020 Red Hat Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""Module with utility and helper functions."""

import asyncio
import base64
import contextlib
import datetime
import json
import logging
import os
import subprocess
import sys
from typing import Dict

import yaml

from mrack.errors import ConfigError, ProvisioningError

logger = logging.getLogger(__name__)

# Singleton context holder
global_context: Dict = {}


def get_config_value(config_dict, key, default=None):
    """Get configuration value or default value from dictionary.

    Usable for dictionaries which can also contain attribute 'default'

    Order of preference:
    * attribute value
    * value of 'default' attribute
    * value of provided default param
    """
    val = config_dict.get(key)
    if val is None:
        val = config_dict.get("default", default)
    return val


def validate_dict_attrs(dct, attr_list, dct_name):
    """Validate that dictionary contains all attributes.

    Based on provided attribute list.
    """
    missing = [a for a in attr_list if a not in dct]
    if missing:
        error = f"""
        '{dct_name} definition:'
        {dct}
        'Is missing required attributes: {missing}'
        """
        raise ConfigError(error)


def json_convertor(obj):
    """Convert object to be useable for json serialization.

    To be used in json.dumps as value for default= so that given object
    is serializable
    """
    if "Binary" in str(obj.__class__):
        return base64.b64encode(obj.data).decode("utf-8")

    if "DateTime" in str(obj.__class__):
        date = datetime.datetime.strptime(str(obj), "%Y%m%dT%H:%M:%S")
        return date.isoformat() + "Z"

    if "Decimal" in str(obj.__class__):
        return str(obj)

    # use string as default
    return str(obj)


def load_json(path):
    """Load JSON file into Python object."""
    with open(os.path.expanduser(path), "r") as file_data:
        data = json.load(file_data)
    return data


def load_yaml(path):
    """Load YAML file into Python object."""
    with open(os.path.expanduser(path), "r") as file_data:
        data = yaml.safe_load(file_data)
    return data


def save_to_json(path, data):
    """Serialize object into JSON file."""
    try:
        with open(os.path.expanduser(path), "w") as output:
            json.dump(data, output, default=json_convertor, indent=2, sort_keys=True)
    except IOError as exc:
        logger.exception(exc)
        sys.exit(1)


@contextlib.contextmanager
def fd_open(filename=None):
    """Use file or stout as output file descriptor."""
    if filename:
        file_d = open(os.path.expanduser(filename), "w")
    else:
        file_d = sys.stdout

    try:
        yield file_d
    finally:
        if file_d is not sys.stdout:
            file_d.close()


def save_yaml(path, yaml_data):
    """
    Write yaml data with file.write(yaml.dump()) to file specified by path.

    If path is not specified use stdout.
    """
    with fd_open(path) as yaml_file:
        yaml_file.write(yaml.dump(yaml_data, default_flow_style=False))


def object2json(obj):
    """Convert object to JSON string."""
    return json.dumps(obj, default=json_convertor, sort_keys=True, indent=4)


def print_obj(obj):
    """Print object as JSON."""
    logger.info(object2json(obj))


def get_host_from_metadata(metadata, name):
    """
    Get host definition from job metadata base on name.

    Returns:
    (host, domain)
    """
    domains = metadata.get("domains", [])
    for domain in domains:
        for host in domain.get("hosts", []):
            if host["name"] == name:
                return host, domain

    return None, None


def is_windows_host(meta_host):
    """
    Return if host is Windows host based on host metadata info.

    Host is windows host if:
    * os starts with 'win' or
    * os_type is 'windows'
    """
    return (
        meta_host.get("os", "").startswith("win")
        or meta_host.get("os_type", "") == "windows"
    )


def get_username(host, meta_host, config):
    """Find username from sources db/metadata/config."""
    username = host.username or meta_host.get("username")
    if is_windows_host(meta_host):
        username = username or "Administrator"

    default_user = get_config_value(config["users"], meta_host["os"])
    username = username or default_user
    return username


def get_password(host, meta_host, config):
    """Find password from sources db/metadata/config."""
    password = host.password or meta_host.get("password")
    multihost_config = config.get("mhcfg")
    if is_windows_host(meta_host) and multihost_config:
        password = password or multihost_config.get("ad_admin_password")
    return password


def get_ssh_key(host, meta_host, config):
    """Find ssh_key path from sources: db/metadata/config."""
    ssh_key_attr = "ssh_key_filename"
    provider_config = config.get(host.provider.name, {})

    ssh_key = meta_host.get(ssh_key_attr)
    ssh_key = ssh_key or provider_config.get(ssh_key_attr)
    ssh_key = ssh_key or config.get(ssh_key_attr)
    return ssh_key


def ssh_to_host(
    host,
    username=None,
    password=None,
    ssh_key=None,
    command=None,
):
    """SSH to the selected host."""
    my_env = os.environ.copy()

    run_args = {
        "env": my_env,
        "shell": True,
    }

    cmd = ["ssh"]
    cmd.extend(["-o", "'StrictHostKeyChecking=no'"])
    cmd.extend(["-o", "'UserKnownHostsFile=/dev/null'"])

    meta_host, _domain = get_host_from_metadata(global_context["metadata"], host.name)
    username = username or get_username(host, meta_host, global_context["config"])
    ssh_key = ssh_key or get_ssh_key(host, meta_host, global_context["config"])

    if username:
        cmd.extend(["-l", username])
    if ssh_key:
        cmd.extend(["-o", "'PasswordAuthentication=no'"])
        cmd.extend(["-i", ssh_key])
    psw_input = None
    if password and not ssh_key:
        cmd.extend(["-o", "'PasswordAuthentication'"])
        psw_input = f"{password}\n"

    cmd.append(host.ip_addr)  # Destination

    if command:
        cmd.append(command)

    cmd = " ".join(cmd)

    logger.info(cmd)
    process = subprocess.Popen(cmd, **run_args)
    process.communicate(input=psw_input)
    return process.returncode == 0


async def exec_async_subprocess(program, args, raise_on_err=True):
    """Util method to execute subprocess asynchronously."""
    process = await asyncio.create_subprocess_exec(
        program,
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    if stdout:
        stdout = stdout.decode()
    if stdout is None:
        stdout = ""
    if stderr:
        stderr = stderr.decode()
    if stdout is None:
        stderr = ""
    if process.returncode != 0 and raise_on_err:
        raise ProvisioningError(stderr)
    return stdout, stderr, process


class NoSuchFileHandler:
    """
    Decorator which change error into ConfigError with custom message.

    error string can contain {path} template.
    """

    def __init__(self, error):
        """Initialize decorator."""
        self.error = error

    def __call__(self, func):
        """Transform FileNotFoundError into ConfigError."""

        def wrapped_f(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except FileNotFoundError as file_error:
                raise ConfigError(
                    self.error.format(path=file_error.filename)
                ) from file_error

        return wrapped_f
