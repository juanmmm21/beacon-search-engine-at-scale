"""Tests del job `train-reranker` (`console/reranker_job.py`) contra
`LocalFilesystemObjectStorage`: el modelo entrenado se publica completo en el
almacenamiento de objetos y es cargable de vuelta por
`LightGBMReranker.load` -- el mismo round-trip que hace una réplica de la
consola al arrancar."""

from __future__ import annotations

from pathlib import Path

from learning_to_rank_reranker.model import LightGBMReranker

from beacon_scale_infra.console.reranker_job import (
    RerankerTrainingConfig,
    RerankerTrainingPipeline,
)
from beacon_scale_infra.storage.local import LocalFilesystemObjectStorage

_BUCKET = "test-bucket"


async def test_trained_model_roundtrips_through_object_storage(tmp_path: Path) -> None:
    storage = LocalFilesystemObjectStorage(tmp_path / "storage")
    config = RerankerTrainingConfig(bucket=_BUCKET, num_queries=10, candidates_per_query=5, seed=7)

    stats = await RerankerTrainingPipeline(config, storage=storage).run()

    assert stats.model_files_uploaded == 2  # model.txt + manifest.json
    assert 0.0 <= stats.ndcg_at_10 <= 1.0
    assert 0.0 <= stats.map_at_10 <= 1.0

    model_dir = tmp_path / "downloaded-model"
    model_dir.mkdir()
    async for entry in storage.list_objects(_BUCKET, prefix="ltr-model/"):
        data = await storage.get_object(_BUCKET, entry.key)
        (model_dir / entry.key.rsplit("/", 1)[1]).write_bytes(data)

    model = LightGBMReranker.load(model_dir)
    assert model is not None
