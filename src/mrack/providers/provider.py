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

"""General Provider interface."""
import asyncio
import logging
from datetime import datetime, timedelta

from mrack.errors import ProvisioningError, ValidationError
from mrack.host import STATUS_OTHER

logger = logging.getLogger(__name__)


class Provider:
    """General Provider interface."""

    def __init__(self, provisioning_config, job_config):
        """Initialize provider."""
        self._name = "dummy"
        self.STATUS_MAP = {"OTHER": STATUS_OTHER}
        self.poll_sleep_initial = 0
        self.poll_sleep = 1
        self.timeout = 60  # minutes
        return

    @property
    def name(self):
        """Get provider name."""
        return self._name

    async def validate_hosts(self, hosts):
        """Validate that host requirements are well specified."""
        raise NotImplementedError()

    async def can_provision(self, hosts):
        """Check that provider has enough resources to provision hosts."""
        raise NotImplementedError()

    async def create_server(self, req):
        """Request and create resource on selected provider."""
        raise NotImplementedError()

    async def is_provisioned(self, resource):
        """
        Check if create_server resource is already provisioned.

        Returns False if not, otherwise returns object for to_host method.
        """
        raise NotImplementedError()

    def req_hostame(self, req):
        """Get hostname part from provisioning requirement object."""
        return req.get('name') or req.get('hostname')

    def reconfigure(self, reqs):
        """Reconfigure itself based on hosts to be provisioned."""
        return

    async def wait_till_provisioned(self, req, resource):
        """
        Wait till server is provisioned.

        Provisioned means that server is in ACTIVE or ERROR state

        State is checked by polling. Polling can be controller via `poll_sleep` and
        `poll_sleep_initial` options. This is useful when provisioning a lot of
        machines as it is better to increase initial poll to not ask to often as
        provisioning resources takes some time.

        Waits till timeout happens. Timeout can be either specified or default provider
        timeout is used.

        Return information about provisioned server.
        """
        start = datetime.now()
        timeout_time = start + timedelta(minutes=self.timeout)

        # do not check the state immediately, it will take some time
        await asyncio.sleep(self.poll_sleep_initial)

        while datetime.now() < timeout_time:
            result = await self.is_provisioned(resource)
            if result:
                break
            await asyncio.sleep(self.poll_sleep)

        done_time = datetime.now()
        prov_duration = (done_time - start).total_seconds()

        hostname = self.req_hostame(req)
        if datetime.now() >= self.timeout:
            logger.warning(
                f"{hostname} was not provisioned within a timeout of"
                f" {self.timeout} mins"
            )
        else:
            logger.info(f"{hostname} was provisioned in {prov_duration:.1f}s")

        return result

    async def provision_hosts(self, hosts, logger=None):
        """Provision hosts based on list of host requirements.

        Main provider method for provisioning.

        First it validates that host requirements are valid and that
        provider has enough resources(quota).

        Then issues provisioning and waits for it succeed. Raises exception if any of
        the servers was not successfully provisioned. If that happens it issues deletion
        of all already provisioned resources.

        Return list of information about provisioned servers.
        """
        if not logger:
            logger = logging.getLogger(self.name)

        logger.info("Validating hosts definitions")
        await self.validate_hosts(hosts)
        logger.info("Host definitions valid")

        logger.info("Checking available resources")
        can = await self.can_provision(hosts)
        if not can:
            raise ValidationError("Not enough resources to provision")
        logger.info("Resource availability: OK")

        self.reconfigure(hosts)

        started = datetime.now()

        count = len(hosts)
        logger.info(f"Issuing provisioning of {count} hosts")
        create_servers = []
        for req in hosts:
            awaitable = self.create_server(req)
            create_servers.append(awaitable)
        create_resps = await asyncio.gather(*create_servers)
        logger.info("Provisioning issued")

        logger.info("Waiting for all hosts to be available")
        wait_servers = []
        for req, create_resp in zip(hosts, create_resps):
            awaitable = self.wait_till_provisioned(req, create_resp)
            wait_servers.append(awaitable)

        server_results = await asyncio.gather(*wait_servers)
        provisioned = datetime.now()
        provi_duration = provisioned - started

        logger.info("All hosts reached provisioning final state (ACTIVE or ERROR)")
        logger.info(f"Provisioning duration: {provi_duration}")

        hosts = [self.to_host(req, srv) for req, srv in zip(hosts, server_results)]

        err_hosts = [host for host in hosts if host.error_obj]
        if err_hosts:
            logger.info("Some host did not start properly")
            for host in err_hosts:
                print(str(host))
            logger.info("Given the error, will delete all hosts")
            await self.delete_hosts(hosts, logger)
            raise ProvisioningError(err_hosts)

        for host in hosts:
            logger.info(host)
        return hosts

    async def delete_host(self, host):
        """Delete provisioned host."""
        raise NotImplementedError()

    async def delete_hosts(self, hosts, logger):
        """Issue deletion of all servers based on previous results from provisioning."""
        logger.info("Issuing deletion")
        delete_servers = []
        for host in hosts:
            awaitable = self.delete_host(host)
            delete_servers.append(awaitable)
        results = await asyncio.gather(*delete_servers)
        logger.info("All servers issued to be deleted")
        return results

    def to_host(self, req, provisioning_result):
        """Transform provisioning result into Host object."""
        raise NotImplementedError()
