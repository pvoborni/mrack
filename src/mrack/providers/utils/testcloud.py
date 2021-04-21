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

"""Module for working with testcloud."""

# https://pagure.io/testcloud


import asyncio
import logging

from testcloud import config as tc_config
from testcloud import instance as tc_instance
from testcloud.exceptions import TestcloudPermissionsError
from testcloud.image import Image

from mrack.utils import exec_async_subprocess

logger = logging.getLogger(__name__)


class Testcloud:
    """Async wrapper supporting most basic testcloud calls."""

    def __init__(self, program="testcloud"):
        """Init the instance."""
        self.program = program

    async def _run_testcloud(self, args, raise_on_err=True):
        """Util method to execute testcloud process."""
        logger.debug(args)
        return await exec_async_subprocess(self.program, args, raise_on_err)

    async def list(self):
        """List instances of testcloud."""
        return await self._run_testcloud(["instance", "list"])

    async def clean(self):
        """Remove non-existing libvirt VMs from testcloud."""
        return await self._run_testcloud(["instance", "clean"])

    async def _instance_command(self, command, instance_name, additional_args=None):
        """Run instance command of test cloud."""
        args = ["instance", command, instance_name]
        if additional_args:
            args.extend(additional_args)
        return await self._run_testcloud(args)

    async def create(
        self,
        instance_name,
        image_url,
        **kwargs,
    ):
        """Start a new testcloud instance."""
        args = ["instance", "create", instance_name]
        args.extend(["--url", image_url])
        if kwargs.get("ram"):
            args.extend(["--ram", kwargs.get("ram")])
        if kwargs.get("vcpus"):
            args.extend(["--vcpus", kwargs.get("vcpus")])
        if kwargs.get("disksize"):
            args.extend(["--disksize", kwargs.get("disksize")])
        if kwargs.get("timeout"):
            args.extend(["--timeout", kwargs["timeout"]])
        if kwargs.get("vnc"):
            args.extend(["--vnc"])
        if kwargs.get("no_graphics"):
            args.extend(["--no_graphics"])
        if kwargs.get("keep"):
            args.extend(["--keep"])
        return await self._run_testcloud(args)

    def info(self, instance_name):
        """Find instance information."""
        logger.debug(f"running info {instance_name}")
        instances = tc_instance.list_instances()
        config_data = tc_config.get_config()
        match = [i for i in instances if i["name"] == instance_name]
        if match:
            inst = match[0]
            logger.debug(inst)
            return {
                "name": inst["name"],
                "ip": inst["ip"],
                "port": inst["port"],
                "state": inst["state"],
                "password": config_data.PASSWORD,
            }
        return None

    async def start(self, instance_name):
        """Stop an instance."""
        return await self._instance_command("start", instance_name)

    async def stop(self, instance_name):
        """Stop an instance."""
        return await self._instance_command("stop", instance_name)

    async def destroy(self, instance_name):
        """Remove an instance."""
        return await self._instance_command("destroy", instance_name, ["-f"])

    async def reboot(self, instance_name):
        """Reboot an instance."""
        return await self._instance_command("reboot", instance_name)

    def _pull_image(self, image_url):
        """Pull image in testcloud image store."""
        print(image_url)
        tc_image = Image(image_url)
        try:
            tc_image.prepare()
            return True
        except TestcloudPermissionsError as error:
            logger.error(error)
            return False

    async def pull_image(self, image_url):
        """Pull image in testcloud image store."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._pull_image, image_url)
