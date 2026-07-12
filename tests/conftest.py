"""Servidor Consul falso para testear `ConsulServiceRegistry` sin necesitar
un agente Consul real: implementa el subconjunto exacto de la API HTTP de
Consul que este cliente usa (`register`/`check pass`/`deregister`/
`health/service`), como una `aiohttp.web.Application` real servida por
`aiohttp.test_utils.TestServer` -- así el test ejercita el mismo transporte
HTTP real que se usaría contra un Consul de verdad, sin las incompatibilidades
de versión de una librería de mocking de terceros (ver `ARCHITECTURE.md`,
nota sobre por qué no se usa `aioresponses`).
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pytest_asyncio
from aiohttp import web
from aiohttp.test_utils import TestServer


def _build_fake_consul_app() -> web.Application:
    services: dict[str, dict[str, Any]] = {}
    passing: dict[str, bool] = {}

    async def register(request: web.Request) -> web.Response:
        body = await request.json()
        service_id = str(body["ID"])
        services[service_id] = body
        passing[service_id] = False
        return web.Response()

    async def check_pass(request: web.Request) -> web.Response:
        check_id = request.match_info["check_id"]
        service_id = check_id.removeprefix("service:")
        if service_id not in services:
            return web.Response(status=404, text="unknown check")
        passing[service_id] = True
        return web.Response()

    async def deregister(request: web.Request) -> web.Response:
        service_id = request.match_info["service_id"]
        services.pop(service_id, None)
        passing.pop(service_id, None)
        return web.Response()

    async def health_service(request: web.Request) -> web.Response:
        service_name = request.match_info["name"]
        only_passing = request.query.get("passing") == "true"
        results = []
        for service_id, body in services.items():
            if body["Name"] != service_name:
                continue
            if only_passing and not passing.get(service_id, False):
                continue
            results.append(
                {
                    "Service": {
                        "ID": service_id,
                        "Service": body["Name"],
                        "Address": body["Address"],
                        "Port": body["Port"],
                        "Meta": body.get("Meta") or {},
                    }
                }
            )
        return web.json_response(results)

    app = web.Application()
    app.router.add_put("/v1/agent/service/register", register)
    app.router.add_put("/v1/agent/check/pass/{check_id}", check_pass)
    app.router.add_put("/v1/agent/service/deregister/{service_id}", deregister)
    app.router.add_get("/v1/health/service/{name}", health_service)
    return app


@pytest_asyncio.fixture
async def fake_consul_base_url() -> AsyncIterator[str]:
    server = TestServer(_build_fake_consul_app())
    await server.start_server()
    try:
        yield str(server.make_url(""))
    finally:
        await server.close()
