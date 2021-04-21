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

"""mrack default app."""
#  pylint: disable=no-name-in-module
import asyncio
import logging
import os
import sys
from functools import update_wrapper

import click

from mrack.actions.destroy import Destroy
from mrack.actions.etchosts import EtcHostsUpdate
from mrack.actions.list import List
from mrack.actions.output import Output
from mrack.actions.ssh import SSH
from mrack.actions.up import Up
from mrack.config import MrackConfig, ProvisioningConfig
from mrack.dbdrivers.file import FileDBDriver
from mrack.errors import (
    ApplicationError,
    ConfigError,
    MetadataError,
    ProviderError,
    ValidationError,
)
from mrack.providers import providers
from mrack.providers.aws import PROVISIONER_KEY as AWS
from mrack.providers.aws import AWSProvider
from mrack.providers.beaker import PROVISIONER_KEY as BEAKER
from mrack.providers.beaker import BeakerProvider
from mrack.providers.openstack import PROVISIONER_KEY as OPENSTACK
from mrack.providers.openstack import OpenStackProvider
from mrack.providers.podman import PROVISIONER_KEY as PODMAN
from mrack.providers.podman import PodmanProvider
from mrack.providers.static import PROVISIONER_KEY as STATIC
from mrack.providers.static import StaticProvider
from mrack.providers.virt import PROVISIONER_KEY as VIRT
from mrack.providers.virt import VirtProvider
from mrack.utils import NoSuchFileHandler, load_yaml

logger = logging.getLogger(__name__)


def async_run(func):
    """Decorate click actions to run as async."""
    func = asyncio.coroutine(func)

    def wrapper(*args, **kwargs):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(func(*args, **kwargs))

    return update_wrapper(wrapper, func)


def init_providers():
    """Register all providers usable in this session."""
    providers.register(OPENSTACK, OpenStackProvider)
    providers.register(AWS, AWSProvider)
    providers.register(BEAKER, BeakerProvider)
    providers.register(PODMAN, PodmanProvider)
    providers.register(STATIC, StaticProvider)
    providers.register(VIRT, VirtProvider)


def init_db(path):
    """Initialize file database."""
    db_file = FileDBDriver(path)
    return db_file


@NoSuchFileHandler(error="Provisioning config file not found: {path}")
def init_prov_config(path):
    """Load and initialize provisioning configuration."""
    config_data = load_yaml(path)
    config = ProvisioningConfig(config_data)
    return config


def init_metadata(ctx, user_defined_path):
    """Load and initialize job metadata."""
    config = ctx.obj[CONFIG]
    meta_path = user_defined_path or config.metadata_path()
    if not meta_path:
        raise ConfigError("Job metadata file path not provided.")
    if not os.path.exists(meta_path):
        raise ConfigError(f"Job metadata file not found: {meta_path}")
    metadata_data = load_yaml(meta_path)
    ctx.obj[METADATA] = metadata_data

    return metadata_data


async def generate_outputs(ctx):
    """Init and run output action."""
    config = ctx.obj[CONFIG]
    output_action = Output()
    await output_action.init(
        ctx.obj[PROV_CONFIG],
        ctx.obj[METADATA],
        ctx.obj[DB],
        config.ansible_inventory_path(),
        config.pytest_multihost_path(),
    )
    await output_action.generate_outputs()


DB = "db"
CONFIG = "config"
PROV_CONFIG = "provconfig"
METADATA = "metadata"


@click.group()
@click.option("-c", "--mrack-config", type=click.Path(exists=True))
@click.option("-p", "--provisioning-config", type=click.Path(exists=True))
@click.option("-d", "--db", "db_file")  # db file may not exist
@click.option("--debug", default=False, is_flag=True)
@click.pass_context
def mrackcli(ctx, mrack_config, provisioning_config, db_file, debug):
    """Multihost human friendly provisioner."""
    if debug:
        logging.getLogger("mrack").setLevel(logging.DEBUG)

    config = MrackConfig(mrack_config)
    config.load()
    db_path = db_file or config.db_path("./.mrackdb.json")
    p_config_path = provisioning_config or config.provisioning_config_path(
        "./provisioning-config.yaml"
    )

    init_providers()
    ctx.ensure_object(dict)
    ctx.obj[CONFIG] = config
    ctx.obj[DB] = init_db(db_path)
    ctx.obj[PROV_CONFIG] = init_prov_config(p_config_path)


@mrackcli.command()
@click.pass_context
@click.option("-m", "--metadata", type=click.Path(exists=True))
@click.option("-p", "--provider", default="openstack")
@async_run
async def up(ctx, metadata, provider):  # pylint: disable=invalid-name
    """Provision hosts.

    Based on provided job metadata file and provisioning configuration.
    """
    init_metadata(ctx, metadata)

    up_action = Up()
    await up_action.init(ctx.obj[PROV_CONFIG], ctx.obj[METADATA], provider, ctx.obj[DB])
    await up_action.provision()

    await generate_outputs(ctx)


@mrackcli.command()
@click.pass_context
@click.option("-m", "--metadata", type=click.Path(exists=True))
@async_run
async def destroy(ctx, metadata):
    """Destroy provisioned hosts."""
    init_metadata(ctx, metadata)
    destroy_action = Destroy()
    await destroy_action.init(ctx.obj[PROV_CONFIG], ctx.obj[METADATA], ctx.obj[DB])
    await destroy_action.destroy()


@mrackcli.command()
@click.pass_context
@click.option("-m", "--metadata", type=click.Path(exists=True))
@async_run
async def output(ctx, metadata):
    """Create outputs - such as Ansible inventory."""
    init_metadata(ctx, metadata)
    await generate_outputs(ctx)


@mrackcli.command()
@click.pass_context
@async_run
async def list(ctx):  # pylint: disable=redefined-builtin
    """List host tracked by."""
    list_action = List()
    list_action.init(ctx.obj[DB])
    list_action.list()


@mrackcli.command()
@click.pass_context
@click.argument("hostname", required=False)
@click.option("-m", "--metadata", type=click.Path(exists=True))
@async_run
async def ssh(ctx, hostname, metadata):
    """SSH to host."""
    init_metadata(ctx, metadata)
    ssh_action = SSH()
    ssh_action.init(ctx.obj[PROV_CONFIG], ctx.obj[METADATA], ctx.obj[DB])
    return ssh_action.ssh(hostname)


@mrackcli.group()
def eh():  # pylint: disable=invalid-name
    """Commands to update /etc/hosts file."""


@eh.command()
@click.pass_context
def add(ctx):
    """Add active hosts to /etc/hosts file."""
    eh_action = EtcHostsUpdate(ctx.obj[DB])
    eh_action.update()


@eh.command()
@click.pass_context
def clear(ctx):
    """Remove all mrack hosts from /etc/hosts file."""
    eh_action = EtcHostsUpdate(ctx.obj[DB])
    eh_action.clear()


def exception_handler(func):
    """
    Top level exception handler.

    For showing nice output to users if exception bubbles up to the top.
    """

    def handle(*args, **kwargs):
        """Handle exceptions."""
        ret_code = 1  # assuming error
        try:
            ret_code = func(*args, **kwargs)
        except (
            FileNotFoundError,
            ConfigError,
            MetadataError,
            ValidationError,
            ProviderError,
            ApplicationError,
        ) as known_error:
            logger.error(known_error)
            sys.exit(1)
        except Exception as exc:
            logger.exception(exc)
            raise exc

        return ret_code

    return handle


@exception_handler
def run():
    """Run the app."""
    mrackcli(obj={})  # pylint: disable=no-value-for-parameter,unexpected-keyword-arg


if __name__ == "__main__":
    run()
