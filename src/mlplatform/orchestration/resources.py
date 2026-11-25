"""Dagster resources. Every asset reads and writes through ``ArtifactStoreResource``,
so no Dagster IO manager is involved and the same code runs against a local
directory or an S3 prefix."""

from dagster import ConfigurableResource

from ..artifacts import ArtifactStore
from ..synthetic import MarketConfig
from ..training import TrainingConfig


class ArtifactStoreResource(ConfigurableResource):
    base_url: str

    def store(self) -> ArtifactStore:
        return ArtifactStore(self.base_url)


class MarketDataResource(ConfigurableResource):
    """Where raw data comes from. Here: the synthetic generator. A real vendor
    feed would replace ``load`` and keep the same two tables."""

    seed: int = 7
    n_entities: int = 40
    start: str = "2021-01-01"
    end: str = "2024-12-31"
    signal: bool = True

    def config(self) -> MarketConfig:
        return MarketConfig(
            seed=self.seed, n_entities=self.n_entities, start=self.start, end=self.end, signal=self.signal
        )


class TrainingResource(ConfigurableResource):
    test_start: str = "2024-01-01"
    horizon_days: int = 5
    embargo_days: int = 5
    min_test_rows: int = 1000
    bootstrap_resamples: int = 200

    def config(self) -> TrainingConfig:
        return TrainingConfig(
            test_start=self.test_start,
            horizon_days=self.horizon_days,
            embargo_days=self.embargo_days,
            min_test_rows=self.min_test_rows,
            bootstrap_resamples=self.bootstrap_resamples,
        )
