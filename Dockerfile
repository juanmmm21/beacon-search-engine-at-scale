# Imagen del worker de crawl distribuido (fase 1). Un único worker por
# contenedor: `docker-compose.yml` lo levanta como servicio escalable
# ('docker compose up -d --scale crawl-worker=N'), no como imagen
# multipropósito -- MinIO, Redis y Consul son contenedores aparte, definidos
# en el mismo docker-compose.yml.
FROM python:3.11-slim

# git es necesario en tiempo de build: web-crawler-scheduler se instala como
# dependencia directa de Git (ver pyproject.toml, sección [tool.hatch.metadata]
# y ARCHITECTURE.md, "Por qué los diez repos originales no se tocan").
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml LICENSE README.md ./
COPY src/ src/

RUN pip install --no-cache-dir .

ENTRYPOINT ["python", "-m", "beacon_scale_infra"]
CMD ["crawl-worker"]
