import os

import click

import dagster._check as check
from dagster._core.instance import DagsterInstance


@click.group(name="instance")
def instance_cli():
    """Commands for working with the current Dagster instance."""


@instance_cli.command(name="info", help="List the information about the current instance.")
def info_command():
    with DagsterInstance.get() as instance:
        home = os.environ.get("DAGSTER_HOME")

        if instance.is_ephemeral:
            check.invariant(
                home is None,
                f'Unexpected state, ephemeral instance but DAGSTER_HOME is set to "{home}"',
            )
            click.echo(
                "$DAGSTER_HOME is not set, using an ephemeral instance. "
                "Run artifacts will only exist in memory and any filesystem access "
                "will use a temp directory that gets cleaned up on exit."
            )
            return

        click.echo(f"$DAGSTER_HOME: {home}\n")

        click.echo("\nInstance configuration:\n-----------------------")
        click.echo(instance.info_str())

        click.echo("\nStorage schema state:\n---------------------")
        click.echo(instance.schema_str())


@instance_cli.command(name="migrate", help="Automatically migrate an out of date instance.")
def migrate_command():
    home = os.environ.get("DAGSTER_HOME")
    if not home:
        click.echo("$DAGSTER_HOME is not set; ephemeral instances do not need to be migrated.")

    click.echo(f"$DAGSTER_HOME: {home}\n")

    with DagsterInstance.get() as instance:
        instance.upgrade(click.echo)

        click.echo(instance.info_str())


@instance_cli.command(name="reindex", help="Rebuild index over historical runs for performance.")
def reindex_command():
    with DagsterInstance.get() as instance:
        home = os.environ.get("DAGSTER_HOME")

        if instance.is_ephemeral:
            click.echo(
                "$DAGSTER_HOME is not set; ephemeral instances cannot be reindexed.  If you "
                "intended to migrate a persistent instance, please ensure that $DAGSTER_HOME is "
                "set accordingly."
            )
            return

        click.echo(f"$DAGSTER_HOME: {home}\n")

        instance.reindex(click.echo)


@instance_cli.group(name="concurrency")
def concurrency_cli():
    """Commands for working with the instance-wide op concurrency."""


@concurrency_cli.command(name="get", help="Get op concurrency limits")
@click.option(
    "--all",
    required=False,
    is_flag=True,
    help="Get info on all instance op concurrency limits",
)
@click.argument("key", required=False)
def get_concurrency(**kwargs):
    with DagsterInstance.get() as instance:
        if kwargs.get("all"):
            keys = instance.event_log_storage.get_concurrency_limited_keys()
            if not keys:
                click.echo(
                    "No concurrency limits set. Run `dagster instance concurrency set <key>"
                    " <limit>` to set limits."
                )
            else:
                click.echo("Concurrency limits:")
                for key in keys:
                    info = instance.event_log_storage.get_concurrency_info(key)
                    total_count = sum(_[1] for _ in info)
                    claimed_count = sum(_[1] for _ in info if _[0] is not None)
                    click.echo(f'"{key}": {claimed_count} / {total_count} slots occupied')
        elif kwargs.get("key"):
            key = kwargs.get("key")
            info = instance.event_log_storage.get_concurrency_info(key)
            total_count = sum(_[1] for _ in info)
            claimed_count = sum(_[1] for _ in info if _[0] is not None)
            click.echo(f'"{key}": {claimed_count} / {total_count} slots occupied')
        else:
            raise click.ClickException(
                "Must either specify a key argument or the `--all` option. Run `dagster instance "
                "concurrency get --help` for more info."
            )


@concurrency_cli.command(name="set", help="Set op concurrency limits")
@click.argument("key", required=True)
@click.argument("limit", required=True, type=click.INT)
def set_concurrency(key, limit):
    with DagsterInstance.get() as instance:
        instance.event_log_storage.allocate_concurrency_slots(key, limit)
        click.echo(f"Set concurrency limit for {key} to {limit}.")
