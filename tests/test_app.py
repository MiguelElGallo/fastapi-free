import json
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest
from fastapi.testclient import TestClient

from app.main import (
    CPU_ROUNDS,
    DATA,
    _aggregate_proof_spans,
    _parse_thread_tokens,
    app,
    create_app,
)


@pytest.fixture()
def client():
    with TestClient(app) as test_client:
        yield test_client


def test_health(client):
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_runtime_reports_benchmark_shape(client):
    response = client.get("/runtime")

    assert response.status_code == 200
    payload = response.json()
    assert isinstance(payload["python"], str)
    assert isinstance(payload["python_version_full"], str)
    assert isinstance(payload["dependencies"], dict)
    for package in ("fastapi", "starlette", "pydantic", "anyio"):
        assert isinstance(payload["dependencies"][package], str)
    assert payload["dataset_size"] >= 1
    assert payload["large_dataset_size"] >= 1
    assert isinstance(payload["thread_tokens"], int)
    assert "py_gil_disabled" in payload
    assert "gil_enabled" in payload
    assert payload["cpu_rounds"] == CPU_ROUNDS
    assert isinstance(payload["prometheus_enabled"], bool)
    assert isinstance(payload["otel_enabled"], bool)


def test_prometheus_is_disabled_by_default(monkeypatch):
    monkeypatch.delenv("BENCH_PROMETHEUS_ENABLED", raising=False)
    monkeypatch.delenv("PROMETHEUS_ENABLED", raising=False)

    with TestClient(create_app()) as test_client:
        runtime = test_client.get("/runtime")

        assert runtime.status_code == 200
        assert runtime.json()["prometheus_enabled"] is False
        assert test_client.get("/metrics").status_code == 404


def test_prometheus_can_be_enabled_with_env(monkeypatch):
    monkeypatch.setenv("BENCH_PROMETHEUS_ENABLED", "1")
    monkeypatch.delenv("PROMETHEUS_ENABLED", raising=False)

    with TestClient(create_app()) as test_client:
        runtime = test_client.get("/runtime")
        metrics = test_client.get("/metrics")

        assert runtime.status_code == 200
        assert runtime.json()["prometheus_enabled"] is True
        assert metrics.status_code == 200
        assert metrics.headers["content-type"].startswith("text/plain")


def test_lookup_returns_in_memory_record(client):
    response = client.get("/lookup/key-42")

    assert response.status_code == 200
    assert response.json()["key"] == "key-42"


def test_lookup_missing_key_returns_404(client):
    response = client.get("/lookup/missing")

    assert response.status_code == 404


def test_lookup_large_contains_payload(client):
    response = client.get("/lookup-large/key-5")

    assert response.status_code == 200
    payload = response.json()
    assert payload["key"] == "key-5"
    assert len(payload["payload"]["values"]) == 96


def test_lookup_preencoded_returns_canonical_json(client):
    response = client.get("/lookup-preencoded/key-7")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/json")
    assert response.content == json.dumps(
        DATA["key-7"], separators=(",", ":"), sort_keys=True
    ).encode("utf-8")


def test_cpu_returns_deterministic_transform(client):
    first = client.get("/cpu/key-10")
    second = client.get("/cpu/key-10")

    assert first.status_code == 200
    assert first.json() == second.json()
    assert first.json()["rounds"] == CPU_ROUNDS


def test_copy_on_write_does_not_mutate_source(client):
    original_score = DATA["key-11"]["score"]

    response = client.get("/cow/key-11")

    assert response.status_code == 200
    assert response.json()["copy_on_write"] is True
    assert response.json()["score"] != original_score
    assert DATA["key-11"]["score"] == original_score


def test_copy_on_write_update_swaps_snapshot(client):
    response = client.put("/cow/key-13")

    assert response.status_code == 200
    assert response.json()["updated"] == "key-13"
    assert client.get("/cow/key-13").status_code == 200


def test_locked_cache_reports_cache_hit(client):
    first = client.get("/locked-cache/key-12")
    second = client.get("/locked-cache/key-12")

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["cached"] is False
    assert second.json()["cached"] is True


def test_proof_batch_barrier_releases_two_participants(client):
    created = client.post(
        "/proof/batches",
        json={"participants": 2, "rounds": 200, "timeout_seconds": 2, "ttl_seconds": 10},
    )

    assert created.status_code == 200
    batch_id = created.json()["batch_id"]
    ready = Barrier(3)

    def request_work(slot: int):
        ready.wait(timeout=2)
        return client.get(f"/proof/batches/{batch_id}/work/{slot}")

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(request_work, slot) for slot in (0, 1)]
        ready.wait(timeout=2)
        responses = [future.result(timeout=5) for future in futures]

    payloads = sorted((response.json() for response in responses), key=lambda item: item["slot"])
    assert [response.status_code for response in responses] == [200, 200]
    assert [payload["slot"] for payload in payloads] == [0, 1]
    assert [payload["key"] for payload in payloads] == ["key-0", "key-1"]
    assert {payload["error"] for payload in payloads} == {None}
    assert len({payload["thread_id"] for payload in payloads}) == 2
    for payload in payloads:
        assert isinstance(payload["checksum"], int)
        assert isinstance(payload["digest"], str)
        assert payload["perf_counter_end_ns"] >= payload["perf_counter_start_ns"]
        assert payload["thread_time_end_ns"] >= payload["thread_time_start_ns"]

    aggregate = client.get(f"/proof/batches/{batch_id}")

    assert aggregate.status_code == 200
    aggregate_payload = aggregate.json()
    assert aggregate_payload["participants_completed"] == 2
    assert aggregate_payload["wall_ns"] > 0
    assert aggregate_payload["sum_thread_cpu_ns"] > 0
    assert aggregate_payload["parallelism_ratio"] >= 0
    assert [span["slot"] for span in aggregate_payload["spans"]] == [0, 1]
    assert client.delete(f"/proof/batches/{batch_id}").json() == {
        "batch_id": batch_id,
        "deleted": True,
    }


def test_proof_batch_parallelism_ratio_calculation():
    aggregate = _aggregate_proof_spans(
        [
            {
                "slot": 1,
                "perf_counter_start_ns": 150,
                "perf_counter_end_ns": 250,
                "thread_time_start_ns": 1_000,
                "thread_time_end_ns": 1_100,
                "error": None,
            },
            {
                "slot": 0,
                "perf_counter_start_ns": 100,
                "perf_counter_end_ns": 300,
                "thread_time_start_ns": 2_000,
                "thread_time_end_ns": 2_200,
                "error": None,
            },
        ]
    )

    assert aggregate["wall_ns"] == 200
    assert aggregate["sum_thread_cpu_ns"] == 300
    assert aggregate["parallelism_ratio"] == pytest.approx(1.5)
    assert aggregate["participants_completed"] == 2
    assert [span["slot"] for span in aggregate["spans"]] == [0, 1]


def test_proof_batch_stale_cleanup_and_delete(client):
    stale = client.post(
        "/proof/batches",
        json={"participants": 1, "rounds": 1, "timeout_seconds": 1, "ttl_seconds": 0.001},
    )

    assert stale.status_code == 200
    stale_batch_id = stale.json()["batch_id"]
    time.sleep(0.02)
    assert client.get(f"/proof/batches/{stale_batch_id}").status_code == 404

    fresh = client.post(
        "/proof/batches",
        json={"participants": 1, "rounds": 1, "timeout_seconds": 1, "ttl_seconds": 10},
    )

    assert fresh.status_code == 200
    fresh_batch_id = fresh.json()["batch_id"]
    assert client.delete(f"/proof/batches/{fresh_batch_id}").json() == {
        "batch_id": fresh_batch_id,
        "deleted": True,
    }
    assert client.get(f"/proof/batches/{fresh_batch_id}").status_code == 404


def test_thread_token_control_endpoint(client):
    response = client.put("/control/thread-tokens/3")

    assert response.status_code == 200
    assert response.json() == {"thread_tokens": 3}
    assert client.get("/control/thread-tokens").json() == {"thread_tokens": 3}


def test_parse_thread_tokens_rejects_invalid_values():
    with pytest.raises(RuntimeError):
        _parse_thread_tokens("0")

    with pytest.raises(RuntimeError):
        _parse_thread_tokens("not-an-int")
