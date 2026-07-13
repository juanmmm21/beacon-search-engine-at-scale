"""Job por lotes `train-reranker` (fase 6): entrena el modelo LTR con
`learning_to_rank_reranker.pipeline.train` **sin modificar** y sube el
directorio de modelo resultante al almacenamiento de objetos de fase 0, de
donde cada réplica de la API de la consola lo descarga al arrancar.

Mismo criterio de job único que `build-index`/`compute-pagerank`/
`shard-index`: se ejecuta una vez (o cuando se quiera reentrenar), nunca como
servicio de larga duración. El entrenamiento es sobre el dataset sintético
determinista de ese paquete (semilla fija -> mismo modelo byte a byte en sus
árboles), independiente del corpus indexado -- exactamente el mismo
entrenamiento que `beacon-search-console` lanza en su bootstrap
(`learning-to-rank-reranker train ... --seed 42`), aquí con el modelo
publicado en `ObjectStorage` en vez de en un `data/` local (ver
`ARCHITECTURE.md`, fase 6).
"""

from __future__ import annotations

import tempfile
from dataclasses import dataclass
from pathlib import Path

from learning_to_rank_reranker.model import LightGBMReranker
from learning_to_rank_reranker.pipeline import train as ltr_train

from beacon_scale_infra.errors import ConsoleServingError
from beacon_scale_infra.protocols import ObjectStorage


@dataclass(frozen=True, slots=True)
class RerankerTrainingConfig:
    """Los tres hiperparámetros del dataset sintético son los mismos valores
    por defecto que usa el bootstrap de `beacon-search-console`."""

    bucket: str = "beacon-scale-dev"
    model_output_prefix: str = "ltr-model"
    num_queries: int = 300
    candidates_per_query: int = 25
    seed: int = 42

    def __post_init__(self) -> None:
        if not self.bucket:
            raise ValueError("bucket no puede estar vacío")
        if not self.model_output_prefix:
            raise ValueError("model_output_prefix no puede estar vacío")
        if self.num_queries <= 1:
            raise ValueError("num_queries debe ser mayor que 1")
        if self.candidates_per_query <= 0:
            raise ValueError("candidates_per_query debe ser positivo")


@dataclass(frozen=True, slots=True)
class RerankerTrainingStats:
    """Resumen final de una ejecución de `RerankerTrainingPipeline.run()`."""

    ndcg_at_10: float
    map_at_10: float
    model_files_uploaded: int


class RerankerTrainingPipeline:
    def __init__(self, config: RerankerTrainingConfig, *, storage: ObjectStorage) -> None:
        self._config = config
        self._storage = storage

    async def run(self) -> RerankerTrainingStats:
        model = LightGBMReranker()
        try:
            report = ltr_train(
                model,
                num_queries=self._config.num_queries,
                candidates_per_query=self._config.candidates_per_query,
                seed=self._config.seed,
            )
        except (ValueError, RuntimeError) as exc:
            raise ConsoleServingError(f"fallo al entrenar el modelo LTR: {exc}") from exc

        uploaded = 0
        with tempfile.TemporaryDirectory(prefix="beacon-scale-ltr-model-") as raw_model_dir:
            model_dir = Path(raw_model_dir)
            model.save(model_dir)
            for path in sorted(model_dir.iterdir()):
                if not path.is_file():
                    continue
                await self._storage.put_object(
                    self._config.bucket,
                    f"{self._config.model_output_prefix}/{path.name}",
                    path.read_bytes(),
                    content_type=("application/json" if path.suffix == ".json" else "text/plain"),
                )
                uploaded += 1

        return RerankerTrainingStats(
            ndcg_at_10=report.validation.ndcg_at_10,
            map_at_10=report.validation.map_at_10,
            model_files_uploaded=uploaded,
        )
