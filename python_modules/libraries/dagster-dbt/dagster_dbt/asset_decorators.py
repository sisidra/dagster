from typing import Any, Callable, Mapping, Optional

from dagster import AssetsDefinition, multi_asset
from dbt.contracts.graph.manifest import WritableManifest

from .asset_utils import get_dbt_multi_asset_args, get_deps
from .utils import ASSET_RESOURCE_TYPES, select_unique_ids_from_manifest


def manifest_to_node_dict(manifest: WritableManifest) -> Mapping[str, Any]:
    raw_dict = manifest.to_dict()
    return {
        **raw_dict["nodes"],
        **raw_dict["sources"],
        **raw_dict["exposures"],
        **raw_dict["metrics"],
    }


def dbt_multi_asset(
    *,
    manifest: WritableManifest,
    select: str = "*",
    exclude: Optional[str] = None,
) -> Callable[..., AssetsDefinition]:
    unique_ids = select_unique_ids_from_manifest(
        select=select, exclude=exclude or "", manifest_parsed=manifest
    )
    dbt_nodes = manifest_to_node_dict(manifest)

    deps = get_deps(
        dbt_nodes,
        selected_unique_ids=unique_ids,
        asset_resource_types=ASSET_RESOURCE_TYPES,
    )
    (
        non_argument_deps,
        outs,
        internal_asset_deps,
    ) = get_dbt_multi_asset_args(
        dbt_nodes=dbt_nodes,
        deps=deps,
    )

    def inner(fn) -> AssetsDefinition:
        asset_definition = multi_asset(
            outs=outs,
            internal_asset_deps=internal_asset_deps,
            non_argument_deps=non_argument_deps,
            compute_kind="dbt",
        )(fn)

        return asset_definition

    return inner
