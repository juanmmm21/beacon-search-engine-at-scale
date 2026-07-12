# Imagen compartida de los workers de fase 1 (crawl distribuido) y fase 2
# (extracción distribuida): mismo paquete `beacon_scale_infra`, mismo
# ENTRYPOINT, y el subcomando de `docker-compose.yml` (`crawl-worker` o
# `extract-worker`) decide qué worker arranca cada contenedor. Un único
# worker por contenedor: `docker-compose.yml` levanta cada servicio como
# escalable de forma independiente ('docker compose up -d --scale
# crawl-worker=N --scale extract-worker=M'), no como imagen multipropósito --
# MinIO, Redis y Consul son contenedores aparte, definidos en el mismo
# docker-compose.yml.
FROM python:3.11-slim

# git es necesario en tiempo de build: web-crawler-scheduler y
# html-content-extractor se instalan como dependencias directas de Git (ver
# pyproject.toml, sección [tool.hatch.metadata] y ARCHITECTURE.md, "Por qué
# los diez repos originales no se tocan").
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml LICENSE README.md ./
COPY src/ src/

RUN pip install --no-cache-dir .

ENTRYPOINT ["python", "-m", "beacon_scale_infra"]
CMD ["crawl-worker"]
