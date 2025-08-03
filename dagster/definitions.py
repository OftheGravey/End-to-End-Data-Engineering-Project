from dagster import (
    Definitions,
    load_assets_from_modules,
    AssetExecutionContext,
    load_asset_checks_from_modules,
)
from pathlib import Path
import textwrap
from typing import Any
from collections.abc import Mapping
from ops.trigger_flink_job_op import streaming_pipeline


# Dagster object that contains the dbt assets and resource
defs = Definitions(
    jobs=[streaming_pipeline]
)