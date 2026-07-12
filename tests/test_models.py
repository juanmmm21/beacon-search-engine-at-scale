"""Tests de validación de los tipos de datos compartidos."""

from __future__ import annotations

import pytest

from beacon_scale_infra.models import ServiceInstance


def test_service_instance_rejects_empty_service_id() -> None:
    with pytest.raises(ValueError, match="service_id"):
        ServiceInstance(service_id="", service_name="shard", host="127.0.0.1", port=9300)


def test_service_instance_rejects_empty_service_name() -> None:
    with pytest.raises(ValueError, match="service_name"):
        ServiceInstance(service_id="shard-0", service_name="", host="127.0.0.1", port=9300)


@pytest.mark.parametrize("port", [0, -1, 65536, 100000])
def test_service_instance_rejects_out_of_range_port(port: int) -> None:
    with pytest.raises(ValueError, match="port"):
        ServiceInstance(service_id="shard-0", service_name="shard", host="127.0.0.1", port=port)


def test_service_instance_base_url() -> None:
    instance = ServiceInstance(service_id="s0", service_name="shard", host="10.0.0.5", port=9300)
    assert instance.base_url == "http://10.0.0.5:9300"
