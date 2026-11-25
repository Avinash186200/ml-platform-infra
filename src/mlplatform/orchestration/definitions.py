"""Jobs, schedules, the promotion sensor and the Definitions object.

Load in Dagster with ``dagster dev -m mlplatform.orchestration.definitions``.
"""

import os

from dagster import (
    AssetKey,
    AssetSelection,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    RunRequest,
    ScheduleDefinition,
    SensorEvaluationContext,
    asset_sensor,
    define_asset_job,
)

from .assets import (
    candidate_model,
    deployed_model,
    feature_snapshot,
    model_artifact,
    model_evaluation,
    publish_feature_snapshot,
    raw_market_data,
    training_set,
)
from .checks import ALL_CHECKS
from .resources import ArtifactStoreResource, MarketDataResource, TrainingResource

ALL_ASSETS = [
    raw_market_data,
    feature_snapshot,
    publish_feature_snapshot,
    training_set,
    candidate_model,
    model_evaluation,
    model_artifact,
    deployed_model,
]

nightly_features_job = define_asset_job(
    "nightly_features_job",
    selection=AssetSelection.assets("raw_prices", "raw_fundamentals", "feature_snapshot", "publish_feature_snapshot"),
    description="Ingest the day's raw data, write and check a new feature snapshot, then publish it.",
    # One at a time: deploy/dagster/dagster.yaml limits runs with this tag to 1.
    tags={"mlpi/serial": "nightly-features"},
)
weekly_retrain_job = define_asset_job(
    "weekly_retrain_job",
    selection=AssetSelection.assets(training_set, candidate_model, model_evaluation, model_artifact),
    description="Rebuild the training set from the current snapshot, fit, evaluate, gate.",
)
promote_model_job = define_asset_job(
    "promote_model_job",
    selection=AssetSelection.assets(deployed_model),
    description="Point models/CURRENT at the newest approved model.",
)

# US closes are published around 21:15 UTC; the nightly build runs at 02:00 UTC
# Tuesday to Saturday, i.e. after each weekday's close.
nightly_features_schedule = ScheduleDefinition(
    name="nightly_features_schedule",
    job=nightly_features_job,
    cron_schedule="0 2 * * 2-6",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.STOPPED,
)
weekly_retrain_schedule = ScheduleDefinition(
    name="weekly_retrain_schedule",
    job=weekly_retrain_job,
    cron_schedule="0 4 * * 0",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.STOPPED,
)


@asset_sensor(
    asset_key=AssetKey("model_artifact"),
    job=promote_model_job,
    name="promote_on_approval",
    default_status=DefaultSensorStatus.STOPPED,
    description="When the gate approves a model (model_artifact materializes), run the promotion job. "
    "Leave this sensor off to require a human to run promote_model_job by hand.",
)
def promote_on_approval(context: SensorEvaluationContext, asset_event):
    model_id = asset_event.dagster_event.event_specific_data.materialization.metadata["model_id"].value
    yield RunRequest(run_key=f"promote-{model_id}-{asset_event.run_id}", tags={"model_id": model_id})


def default_resources() -> dict:
    return {
        "artifacts": ArtifactStoreResource(base_url=os.environ.get("MLPI_ARTIFACTS_URL", "data/artifacts")),
        "market": MarketDataResource(),
        "training": TrainingResource(),
    }


defs = Definitions(
    assets=ALL_ASSETS,
    asset_checks=ALL_CHECKS,
    jobs=[nightly_features_job, weekly_retrain_job, promote_model_job],
    schedules=[nightly_features_schedule, weekly_retrain_schedule],
    sensors=[promote_on_approval],
    resources=default_resources(),
)
