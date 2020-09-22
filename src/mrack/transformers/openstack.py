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

"""OpenStack transformer module."""
import logging

from mrack.errors import ProvisioningConfigError
from mrack.transformers.transformer import Transformer
from mrack.utils import get_config_value, print_obj

logger = logging.getLogger(__name__)

CONFIG_KEY = "openstack"


class OpenStackTransformer(Transformer):
    """OpenStack transformer."""

    _config_key = CONFIG_KEY
    _required_config_attrs = ["flavors", "networks", "images", "keypair"]

    async def init_provider(self):
        """Initialize associate provider."""
        images = self.config["images"].values()
        await self._provider.init(image_names=images)

    def _get_flavor(self, host):
        """Get flavor by host group."""
        # TODO: add sizes

        return get_config_value(self.config["flavors"], host["group"])

    def _is_network_type(self, name):
        """Check if name is a configured network type in provisioning config."""
        nt = self.config["networks"].get(name)
        return bool(nt)

    def _aggregate_networks(self, hosts):
        """
        Get how many host require each used network type.

        Returns: dict where keys are network types and values are total count.
        """
        nts = {}
        for host in hosts:
            # skip hosts which have low-level network names defined
            # this can be extended to pick network type based on the network name
            names = host.get("networks")
            if names:
                continue
            nt = host.get("network")
            if not self._is_network_type(nt):
                continue

            count = nts.get(nt, 0)
            count += 1
            nts[nt] = count
        return nts

    def _pick_network(self, network_type, count):
        """
        Pick network based network type and needed amount.

        Usable network with most IPs available is picked.
        """
        possible_networks = self.config["networks"][network_type]
        networks = [self._provider.get_network(net) for net in possible_networks]
        usable = []
        for network in networks:
            ips = self._provider.get_ips(ref=network.get("id"))
            available = ips["total_ips"] - ips["used_ips"]
            if available > count:
                usable.append((network["name"], available))

        if not usable:
            logger.error(
                f"Error: no usable network for {count} hosts with {network_type}"
            )
            return None

        # sort networks by number of available IPs
        usable = sorted(usable, key=lambda u: u[1])
        logger.info(usable)
        return usable[-1][0]  # Pick the one with most IPs

    def translate_network_types(self, hosts):
        """Pick the right OpenStack networks for all hosts.

        Pick the network based on network type, networks configured for the
        type and the available IP addresses. Process all hosts to
        be able to pick the network which have enough addresses for all hosts.

        All hosts will have either "networks" attribute or "network"
        host attribute set with OpenStack network name or ID.
        """
        nt_requirements = self._aggregate_networks(hosts)
        nt_map = {}
        for network_type, count in nt_requirements.items():
            network_name = self._pick_network(network_type, count)
            nt_map[network_type] = network_name

        for host in hosts:
            # skip hosts which have low-level network names defined
            names = host.get("networks")
            if names:
                continue

            nt = host.get("network")

            # skip if nt is not network type
            if not self.config["networks"].get(nt):
                continue

            network_name = nt_map[nt]
            host["network"] = network_name

    def _get_network_type(self, host):
        """Get network type from host object definition.

        If host object doesn't have it defined, then get it from metadata or
        provisioning config.
        """
        network_type = host.get("network")
        default_network = self.config.get("default_network")
        if network_type is None:
            network_type = self._metadata.get("network", default_network)
        if not network_type:
            raise ProvisioningConfigError(
                "No network type specified and project doesn't have default "
                "network type (property 'default_network') specified in "
                "provisioning config."
            )
        return network_type

    def _get_image(self, os):
        """Get image name by OS name from provisioning config."""
        return get_config_value(self.config["images"], os)

    def create_host_requirement(self, host):
        """Create single input for OpenStack provisioner."""
        return {
            "name": host["name"],
            "flavor": self._get_flavor(host),
            "image": self._get_image(host["os"]),
            "key_name": self.config["keypair"],
            "network": self._get_network_type(host),
        }

    def create_host_requirements(self):
        """Create inputs for all host for OpenStack provisioner.

        This includes picking the right network and checking if it has
        available IP addresses.
        """
        reqs = [self.create_host_requirement(host) for host in self.hosts]
        print_obj(reqs)
        self.translate_network_types(reqs)
        return reqs
