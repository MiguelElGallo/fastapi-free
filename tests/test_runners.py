import pytest

from scripts.host_native_common import (
    HostNativeValidationError,
    parse_k6_duration_seconds,
    validate_host_native_k6,
    validate_loopback_base_url,
    wall_timeout_seconds,
)
from scripts.run_frontier import classify_cell


def test_parse_k6_duration_seconds_accepts_compound_values():
    assert parse_k6_duration_seconds("500ms") == pytest.approx(0.5)
    assert parse_k6_duration_seconds("30s") == pytest.approx(30.0)
    assert parse_k6_duration_seconds("1m30s") == pytest.approx(90.0)
    assert parse_k6_duration_seconds("1h2m3s") == pytest.approx(3723.0)


def test_parse_k6_duration_seconds_rejects_invalid_values():
    with pytest.raises(ValueError):
        parse_k6_duration_seconds("")
    with pytest.raises(ValueError):
        parse_k6_duration_seconds("30")
    with pytest.raises(ValueError):
        parse_k6_duration_seconds("1s garbage")


def test_wall_timeout_adds_request_timeout_and_grace():
    assert wall_timeout_seconds("30s", "5s", grace_seconds=15) == pytest.approx(50.0)


def test_validate_loopback_base_url_rejects_container_networking():
    assert validate_loopback_base_url("http://127.0.0.1:8000") == "http://127.0.0.1:8000"

    with pytest.raises(HostNativeValidationError):
        validate_loopback_base_url("http://host.docker.internal:8000")
    with pytest.raises(HostNativeValidationError):
        validate_loopback_base_url("http://localhost:8000")
    with pytest.raises(HostNativeValidationError):
        validate_loopback_base_url("http://127.0.0.1")


def test_validate_host_native_k6_rejects_docker_launchers():
    assert validate_host_native_k6("k6") == "k6"

    with pytest.raises(HostNativeValidationError):
        validate_host_native_k6("docker")
    with pytest.raises(HostNativeValidationError):
        validate_host_native_k6("scripts/k6-docker")
    with pytest.raises(HostNativeValidationError):
        validate_host_native_k6("k6 run")


def test_classify_cell_requires_clean_slo_compliant_load():
    valid = classify_cell(
        {
            "requests_per_sec": 9.7,
            "failed_requests_rate": 0.0,
            "checks_rate": 1.0,
            "dropped_iterations": 0.0,
            "duration_p99_ms": 400.0,
        },
        target_rps=10,
        min_achieved_ratio=0.95,
        max_failed_rate=0.001,
        p99_slo_ms=1000.0,
    )
    assert valid == {"valid": True, "classification": "valid", "reasons": []}

    invalid = classify_cell(
        {
            "requests_per_sec": 9.7,
            "failed_requests_rate": 0.0,
            "checks_rate": 1.0,
            "dropped_iterations": 1.0,
            "duration_p99_ms": 1200.0,
        },
        target_rps=10,
        min_achieved_ratio=0.95,
        max_failed_rate=0.001,
        p99_slo_ms=1000.0,
    )
    assert invalid["valid"] is False
    assert invalid["classification"] == "saturated"
    assert invalid["reasons"] == ["dropped_iterations", "p99_slo"]
