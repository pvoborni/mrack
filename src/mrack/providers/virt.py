# Copyright 2021 Red Hat Inc.
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

"""Virt provider."""

import asyncio
import getpass
import grp
import logging
import os
from datetime import datetime

from mrack.errors import ValidationError
from mrack.host import STATUS_ACTIVE, STATUS_OTHER
from mrack.providers.provider import STRATEGY_ABORT, Provider
from mrack.providers.utils.testcloud import Testcloud

logger = logging.getLogger(__name__)

PROVISIONER_KEY = "virt"


class VirtProvider(Provider):
    """Virt provisioning provider."""

    def __init__(self):
        """Initialize provider."""
        self._name = PROVISIONER_KEY
        self.dsp_name = "Virt"
        self.strategy = STRATEGY_ABORT
        self.testcloud = Testcloud()
        self.status_map = {
            "running": STATUS_ACTIVE,
            "shutoff": STATUS_OTHER,
            "undefined": STATUS_OTHER,
        }

    async def init(self):
        """Initialize Virt provider with data from config."""
        logger.info(f"{self.dsp_name}: Initializing provider")
        login_start = datetime.now()
        login_end = datetime.now()
        login_duration = login_end - login_start
        logger.info(f"{self.dsp_name}: Init duration {login_duration}")

    async def prepare_provisioning(self, reqs):
        """Prepare provisioning."""
        # Check that current user belongs to testcloud group
        group = "testcloud"
        uname = getpass.getuser()
        _name, _pass, gid, _members = grp.getgrnam(group)
        groups = os.getgroups()
        if gid not in groups:
            raise ValidationError(
                f"Error: Current user is not a member of a {group} group.\n"
                "Make sure that testcloud is installed and add the user to the "
                "group, e.g. by:\n"
                f"  sudo usermod -a -G {group} {uname}"
            )

        # Pulling images ahead so that the provider doesn't download the same
        # image more than once
        pull = set()
        for req in reqs:
            pull.add(req["image_url"])

        awaitables = []
        for url in pull:
            logger.info(f"{self.dsp_name}: Pulling image '{url}'")
            awaitables.append(self.testcloud.pull_image(url))

        pull_results = await asyncio.gather(*awaitables)
        success = all(pull_results)

        if not success:
            logger.error(f"{self.dsp_name}: Pulling of images failed")
        else:
            logger.info(f"{self.dsp_name}: Images prepared")

        return success

    async def validate_hosts(self, reqs):
        """Validate that host requirements are well specified."""
        return bool(reqs)  # no specific validation yet

    async def can_provision(self, hosts):
        """Check that provider has enough resources to provision hosts."""
        # We'd need to check available memory, this is TODO
        return bool(hosts)

    async def create_server(self, req):
        """Request and create resource on Virt provider."""
        hostname = req["name"]
        logger.info(f"{self.dsp_name}: Creating virtual machine for host: {hostname}")

        host_id = req["run_id"] + "-" + hostname
        out, err, _proc = await self.testcloud.create(
            host_id,
            **req,
        )
        info = self.testcloud.info(host_id)
        info["id"] = host_id
        info["name"] = hostname
        info["output"] = out
        if info["state"] != "running":
            info["error"] = err
        return info

    async def wait_till_provisioned(self, resource):
        """Wait till resource is provisioned."""
        return resource

    async def delete_host(self, host_id):
        """Delete provisioned host."""
        _out, _err, _proc = await self.testcloud.destroy(host_id)
        return True

    def prov_result_to_host_data(self, prov_result):
        """Get needed host information from podman provisioning result."""
        result = {}

        print(prov_result)

        result["id"] = prov_result.get("id")
        result["name"] = prov_result.get("name")
        result["addresses"] = [prov_result.get("ip")]
        result["status"] = prov_result["state"]
        result["fault"] = prov_result.get("error")
        result["password"] = prov_result["password"]

        return result
