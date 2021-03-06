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

"""Ansible inventory output module."""

from copy import deepcopy

from mrack.errors import ConfigError
from mrack.outputs.utils import resolve_hostname
from mrack.utils import (
    get_host_from_metadata,
    get_password,
    get_ssh_key,
    get_username,
    is_windows_host,
    save_yaml,
)

DEFAULT_INVENTORY_PATH = "mrack-inventory.yaml"
DEFAULT_INVENTORY_LAYOUT = {"all": {"children": {}, "hosts": {}}}


def copy_meta_attrs(host, meta_host, attrs):
    """Copy attributes from host metadata entry and prefixing them with meta."""
    for attr in attrs:
        val = meta_host.get(attr)
        if val:
            host[f"meta_{attr}"] = val


def ensure_all_group(inventory):
    """Ensure that inventory has group "all" with both "hosts" and "children"."""
    if "all" not in inventory:
        inventory["all"] = {
            "children": {},
            "hosts": {},
        }
    all_group = inventory["all"]
    if "children" not in all_group:
        all_group["children"] = {}
    if "hosts" not in all_group:
        all_group["hosts"] = {}
    return all_group


def get_group(inventory, groupname):
    """Get group from inventory, return group or None."""
    groups = inventory.keys()
    found = None
    for g_name in groups:
        group = inventory[g_name]
        if g_name == groupname:
            found = group
        else:
            if "children" in group:
                found = get_group(group["children"], groupname)
        if found:
            break
    return found


def add_to_group(inventory, groupname, hostname):
    """
    Find a group in inventory layout and add a host to it.

    Returns group or None if it doesn't exist.
    """
    group = get_group(inventory, groupname)
    if not group:
        return None

    if "hosts" not in group:
        group["hosts"] = {}
    group["hosts"][hostname] = {}
    return group


def add_group(inventory, groupname, hostname=None):
    """Add a group to inventory, optionally add it with hostname."""
    all_group = inventory["all"]
    group = {}
    if hostname:
        group["hosts"] = {hostname: {}}
    all_group["children"][groupname] = group
    return group


class AnsibleInventoryOutput:
    """
    Generate Ansible inventory with provisioned machines.

    The resulting inventory combines data from provisioning config, job metadata
    files and information in DB.
    """

    def __init__(self, config, db, metadata, path=DEFAULT_INVENTORY_PATH):
        """Init the output module."""
        self._config = config
        self._db = db
        self._metadata = metadata
        self._path = path

    def create_ansible_host(self, name):
        """Create host entry for Ansible inventory."""
        meta_host, meta_domain = get_host_from_metadata(self._metadata, name)
        db_host = self._db.hosts[name]

        ip = db_host.ip
        ansible_host = resolve_hostname(ip) or ip

        python = (
            self._config["python"].get(meta_host["os"])
            or self._config["python"]["default"]
        )

        ansible_user = get_username(db_host, meta_host, self._config)
        password = get_password(db_host, meta_host, self._config)
        ssh_key = get_ssh_key(db_host, meta_host, self._config)

        # Common attributes
        host_info = {
            "ansible_host": ansible_host,
            "ansible_python_interpreter": python,
            "ansible_user": ansible_user,
            "meta_fqdn": name,
            "meta_domain": meta_domain["name"],
            "meta_provider_id": db_host.id,
            "meta_ip": ip,
            "meta_dc_record": ",".join(
                "DC=%s" % dc for dc in meta_domain["name"].split(".")
            ),
        }
        copy_meta_attrs(host_info, meta_host, ["os", "role", "netbios"])

        if "parent" in meta_domain:
            host_info["parent_domain"] = meta_domain["parent"]

        if ssh_key:
            host_info["ansible_ssh_private_key_file"] = ssh_key

        if password:
            host_info["ansible_password"] = password

        if is_windows_host(meta_host):
            host_info.update(
                {
                    "ansible_port": 5986,
                    "ansible_connection": "winrm",
                    "ansible_winrm_server_cert_validation": "ignore",
                    "meta_domain_level": meta_host.get("domain_level", "top"),
                }
            )
        return host_info

    def create_inventory(self):
        """Create the Ansible inventory in dict form."""
        provisioned = self._db.hosts
        inventory = deepcopy(
            self._config.get("inventory_layout", DEFAULT_INVENTORY_LAYOUT)
        )
        if type(inventory) is not dict:
            raise ConfigError("Inventory layout should be a dictionary")
        all_group = ensure_all_group(inventory)

        for host in provisioned.values():
            meta_host, meta_domain = get_host_from_metadata(self._metadata, host.name)

            # Groups can be defined in both "groups" and "group" variable.
            groups = meta_host.get("groups", [])
            group = meta_host.get("group")
            if group and group not in groups:
                groups.append(group)

            # Add only a reference custom groups
            for group in groups:
                added = add_to_group(inventory, group, host.name)
                if not added:  # group doesn't exist
                    add_group(inventory, group, host.name)

            # Main record belongs in "all" group
            all_group["hosts"][host.name] = self.create_ansible_host(host.name)
        return inventory

    def create_output(self):
        """Create the target output file."""
        inventory = self.create_inventory()
        save_yaml(self._path, inventory)
        return inventory
