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
import socket
from datetime import datetime, timedelta

from mrack.errors import ProvisioningError, ValidationError
from mrack.host import STATUS_ACTIVE, STATUS_OTHER, Host
from mrack.utils import ssh_to_host

logger = logging.getLogger(__name__)

STRATEGY_ABORT = "abort"
STRATEGY_RETRY = "retry"
RET_CODE = 0
HOST_OBJ = 1


class Provider:
    """General Provider interface."""

    def __init__(self):
        """Initialize provider."""
        self._name = "dummy"
        self.dsp_name = "Dummy"
        self.max_attempts = 1
        self.strategy = STRATEGY_ABORT
        self.status_map = {"OTHER": STATUS_OTHER}

    @property
    def name(self):
        """Get provider name."""
        return self._name

    async def validate_hosts(self, reqs):
        """Validate that host requirements are well specified."""
        raise NotImplementedError()

    async def can_provision(self, hosts):
        """Check that provider has enough resources to provision hosts."""
        raise NotImplementedError()

    async def create_server(self, req):
        """Request and create resource on selected provider."""
        raise NotImplementedError()

    async def wait_till_provisioned(self, resource):
        """Wait till resource is provisioned."""
        raise NotImplementedError()

    async def prepare_provisioning(self, reqs):
        """Prepare provisioning."""
        raise NotImplementedError()

    async def _wait_for_ssh(self, host, port=22, timeout=1200):
        """
        Wait until a port starts accepting TCP connections.

        Args:
            port (int): Port number.
            host (Host): Host object to get its address on which the port should exist.
            timeout (float): In seconds. How long to wait before raising errors.
        Raises:
            TimeoutError: The port isn't accepting connection after specified `timeout`.
        """
        start_time = datetime.now()
        info_msg = (
            f"{self.dsp_name}: Waiting for the port {port} "
            f"on host {host.ip_addr} to start accepting connections"
        )
        logger.info(info_msg)

        while True:
            try:
                with socket.create_connection((host.ip_addr, port), timeout=timeout):
                    logger.info(
                        f"{self.dsp_name}: Port {port} on host "
                        f" {host.ip_addr} is now open"
                    )
                    break
            except OSError:
                await asyncio.sleep(10)
                logger.debug(info_msg)
                if datetime.now() - start_time >= timedelta(minutes=timeout):
                    logger.error(
                        f"{self.dsp_name}: Waited too long for the port {port} "
                        f"on host {host.ip_addr} to start accepting connections"
                    )
                    break

        # Wait also for the ssh key to be accepted for a half timeout time
        start_ssh = datetime.now()

        while True:
            res = ssh_to_host(host, command="echo mrack")
            duration = (datetime.now() - start_ssh).total_seconds()
            if res:
                logger.info(
                    f"{self.dsp_name}: SSH to host '{host.ip_addr}' successful "
                    f"after {duration:.1f}s"
                )
                break

            if datetime.now() - start_ssh >= timedelta(minutes=(timeout / 2)):
                logger.error(
                    f"{self.dsp_name}: SSH to host '{host.ip_addr}' "
                    f"timed out after {duration:.1f}s"
                )
                break

            # wait 10 seconds to retry the ssh connection
            await asyncio.sleep(10)

        return res, host

    async def _provision_base(self, reqs):  # pylint: disable=too-many-locals
        """Provision hosts based on list of host requirements.

        Main function which does provisioning and not any validation.
        """
        logger.info(f"{self.dsp_name}: Validating hosts definitions")
        if not reqs:
            raise ProvisioningError(
                f"{self.dsp_name}: Can not continue with empty requirement for provider"
            )

        await self.validate_hosts(reqs)
        logger.info(f"{self.dsp_name}: Host definitions valid")

        logger.info(f"{self.dsp_name}: Checking available resources")

        if not await self.can_provision(reqs):
            raise ValidationError(f"{self.dsp_name}: Not enough resources to provision")
        logger.info(f"{self.dsp_name}: Resource availability: OK")
        started = datetime.now()

        logger.info(f"{self.dsp_name}: Issuing provisioning of {len(reqs)} host(s)")
        create_servers = []
        for req in reqs:
            awaitable = self.create_server(req)
            create_servers.append(awaitable)
        create_resps = await asyncio.gather(*create_servers)
        logger.info(f"{self.dsp_name}: Provisioning issued")

        logger.info(f"{self.dsp_name}: Waiting for all hosts to be active")
        wait_servers = []
        for create_resp in create_resps:
            awaitable = self.wait_till_provisioned(create_resp)
            wait_servers.append(awaitable)

        server_results = await asyncio.gather(*wait_servers)
        provisioned = datetime.now()

        logger.info(
            f"{self.dsp_name}: "
            "All hosts reached provisioning final state (ACTIVE or ERROR)"
        )
        logger.info(f"{self.dsp_name}: Provisioning duration: {provisioned - started}")

        hosts = [self.to_host(srv) for srv in server_results]
        error_hosts = await self.parse_error_hosts(hosts)
        active_hosts = [h for h in hosts if h not in error_hosts]
        # check ssh connectivity to succeeded hosts
        wait_ssh = []
        for host in active_hosts:
            awaitable = self._wait_for_ssh(host)
            wait_ssh.append(awaitable)

        ssh_results = await asyncio.gather(*wait_ssh)
        # We distinguish the success hosts and new error hosts from active by using:
        # res[RET_CODE] 0 - the result of operation returned from self._wait_for_ssh()
        # res[HOST_OBJ] 1 - the host object returned from self._wait_for_ssh()
        success_hosts = []
        for res in ssh_results:
            if res[RET_CODE]:
                success_hosts.append(res[HOST_OBJ])
            else:
                res[HOST_OBJ].error = (
                    "Could not establish ssh connection to host "
                    f"{res[HOST_OBJ].host_id} with IP {res[HOST_OBJ].ip_addr}"
                )
                error_hosts.append(res[HOST_OBJ])

        missing_reqs = [
            req for req in reqs if req["name"] in [host.name for host in error_hosts]
        ]
        return (success_hosts, error_hosts, missing_reqs)

    async def provision_hosts(self, reqs):
        """Provision hosts based on list of host requirements.

        Main provider method for provisioning.

        First it validates that host requirements are valid and that
        provider has enough resources(quota).

        Then issues provisioning and waits for it succeed. Raises exception if any of
        the servers was not successfully provisioned. If that happens it issues deletion
        of all already provisioned resources.

        Return list of information about provisioned servers.
        """
        logger.info(f"{self.dsp_name}: Preparing provider resources")
        await self.prepare_provisioning(reqs)

        logger.info(f"{self.dsp_name}: Validating hosts definitions")
        await self.validate_hosts(reqs)

        if self.strategy == STRATEGY_RETRY:
            success_hosts, error_hosts, _missing_reqs = await self.strategy_retry(reqs)
        else:
            success_hosts, error_hosts, _missing_reqs = await self.strategy_abort(reqs)

        if error_hosts:
            hosts_to_delete = success_hosts + error_hosts
            await self.abort_and_delete(hosts_to_delete, error_hosts)

        logger.info(f"{self.dsp_name}: Printing provisioned hosts")
        for host in success_hosts:
            logger.info(f"{self.dsp_name}: {host}")

        return success_hosts

    async def strategy_retry(self, reqs):
        """Provisioning strategy to try multiple times to provision a host."""
        missing_reqs = reqs
        attempts = 0
        success_hosts = []
        error_hosts = []

        while missing_reqs:
            if attempts >= self.max_attempts:
                logger.error(f"Max attempts({self.max_attempts}) reached. Aborting.")
                break

            attempts += 1
            s_hosts, error_hosts, missing_reqs = await self._provision_base(
                missing_reqs
            )
            success_hosts.extend(s_hosts)

            if error_hosts:
                count = len(error_hosts)
                err = f"{count} hosts were not provisioned properly, deleting."
                logger.info(f"{self.dsp_name}: {err}")
                for host in error_hosts:
                    logger.error(f"{self.dsp_name}: Error: {str(host.error)}")
                await self.delete_hosts(error_hosts)
                logger.info("Retrying to provision these hosts.")

        return success_hosts, error_hosts, missing_reqs

    async def strategy_abort(self, reqs):
        """Provisioning strategy to try once and then abort."""
        return await self._provision_base(reqs)

    async def parse_error_hosts(self, hosts):
        """Parse provisioning errors from provider result."""
        errors = []
        logger.debug(f"{self.dsp_name}: Checking provisioned hosts for errors")
        for host in hosts:
            logger.debug(
                f"{self.dsp_name}: Host - {host.host_id}\tStatus - {host.status}"
            )

            if host.status != STATUS_ACTIVE:
                errors.append(host)

        return errors

    async def abort_and_delete(self, hosts_to_delete, error_hosts):
        """Delete hosts and abort provisioning with an error."""
        logger.info(f"{self.dsp_name}: Aborting provisioning due to error.")
        for host in error_hosts:
            logger.error(f"{self.dsp_name}: Error: {str(host.error)}")

        logger.info(f"{self.dsp_name}: Given the error, will delete hosts")
        await self.delete_hosts(hosts_to_delete)
        raise ProvisioningError(error_hosts)

    async def delete_host(self, host_id):
        """Delete provisioned host."""
        raise NotImplementedError()

    async def delete_hosts(self, hosts):
        """Issue deletion of all servers based on previous results from provisioning."""
        logger.info(f"{self.dsp_name}: Issuing deletion")

        delete_servers = []
        for host in hosts:
            awaitable = self.delete_host(host.host_id)
            delete_servers.append(awaitable)
        results = await asyncio.gather(*delete_servers)
        logger.info(f"{self.dsp_name}: All servers issued to be deleted")
        return results

    def prov_result_to_host_data(self, prov_result):
        """Transform provisioning result to needed host data."""
        raise NotImplementedError()

    def to_host(self, provisioning_result, username=None):
        """Transform provisioning result into Host object."""
        host_info = self.prov_result_to_host_data(provisioning_result)

        host = Host(
            self,
            host_info.get("id"),
            host_info.get("name"),
            host_info.get("addresses"),
            self.status_map.get(host_info.get("status"), STATUS_OTHER),
            provisioning_result,
            username=username,
            password=host_info.get("password"),
            error_obj=host_info.get("fault"),
        )
        return host
