import os
from typing import Any, Dict

from dagster import OpExecutionContext, ResourceParam, load_assets_from_package_module
from dagster._utils import file_relative_path
from dagster_dbt.asset_decorators import dbt_multi_asset
from dagster_dbt.cli.resources import DbtCliClient
from dbt.contracts.graph.manifest import WritableManifest

from . import activity_analytics, core, recommender

CORE = "core"
ACTIVITY_ANALYTICS = "activity_analytics"
RECOMMENDER = "recommender"
DBT_PROJECT_DIR = file_relative_path(__file__, "../../dbt_project")
DBT_PROFILES_DIR = DBT_PROJECT_DIR + "/config"

core_assets = load_assets_from_package_module(package_module=core, group_name=CORE)

activity_analytics_assets = load_assets_from_package_module(
    package_module=activity_analytics,
    key_prefix=["snowflake", ACTIVITY_ANALYTICS],
    group_name=ACTIVITY_ANALYTICS,
)

recommender_assets = load_assets_from_package_module(
    package_module=recommender, group_name=RECOMMENDER
)

# dbt_assets = load_assets_from_dbt_manifest(
#     json.load(open(os.path.join(DBT_PROJECT_DIR, "target", "manifest.json"), encoding="utf-8")),
#     io_manager_key="warehouse_io_manager",
#     # the schemas are already specified in dbt, so we don't need to also specify them in the key
#     # prefix here
#     key_prefix=["snowflake"],
#     source_key_prefix=["snowflake"],
# )

manifest_path = os.path.join(DBT_PROJECT_DIR, "target", "manifest.json")
manifest = WritableManifest.read_and_check_versions(manifest_path)


@dbt_multi_asset(manifest=manifest)
def dbt_assets(context: OpExecutionContext, dbt: ResourceParam[DbtCliClient]):
    kwargs: Dict[str, Any] = {}
    # in the case that we're running everything, opt for the cleaner selection string
    if len(context.selected_output_names) == len(outs):
        kwargs["select"] = select
        kwargs["exclude"] = exclude
    else:
        # for each output that we want to emit, translate to a dbt select string by converting
        # the out to its corresponding fqn
        kwargs["select"] = [
            ".".join(fqns_by_output_name[output_name])
            for output_name in context.selected_output_names
        ]

    dbt.cli_stream_json(
        command="run",
    )


dbt_assets = dbt_assets.with_attributes(can_subset=True)
