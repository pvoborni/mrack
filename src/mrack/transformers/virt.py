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

"""Virt transformer module."""

import secrets

from mrack.transformers.transformer import Transformer

CONFIG_KEY = "virt"


class VirtTransformer(Transformer):
    """Virt transformer."""

    _config_key = CONFIG_KEY
    _required_config_attrs = ["images", "options", "groups"]

    def __init__(self):
        """Initialize Virt transformer."""
        self.run_id = secrets.token_urlsafe()[:6]

    async def init_provider(self):
        """Initialize associate provider and transformer display name."""
        self.dsp_name = "Virt"
        await self._provider.init()

    def _get_host_option(self, host, name):
        default_options = self.config["options"]
        group_options = self.config["groups"].get(host["group"], {})
        val = host.get(name) or group_options.get(name) or default_options.get(name)
        if val:
            val = str(val)
        return val

    def create_host_requirement(self, host):
        """Create single input for podman provisioner."""
        req = {
            "name": host["name"],
            "run_id": self.run_id,
            "image_url": self._get_image(host["os"]),
        }
        for option in [
            "ram",
            "vcpus",
            "disksize",
            "timeout",
            "vnc",
            "no_graphics",
            "keep",
        ]:
            req[option] = self._get_host_option(host, option)
        return req
