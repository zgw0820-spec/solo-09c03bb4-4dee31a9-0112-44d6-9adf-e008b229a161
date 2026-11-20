"""HTTP-level tests using FastAPI's TestClient and the fake writer."""

from __future__ import annotations

import importlib

import pytest
from fastapi.testclient import TestClient

from app.db import Database
from app.service import RotationService


@pytest.fixture()
def client(workspace, monkeypatch):
    settings, writer = workspace
    db = Database(settings.db_path)
    db.initialize()
    service = RotationService(settings, db, writer)

    main = importlib.import_module("app.main")
    monkeypatch.setattr(main, "settings", settings)
    monkeypatch.setattr(main, "db", db)
    monkeypatch.setattr(main, "service", service)
    with TestClient(main.app) as test_client:
        yield test_client, writer, settings, service


def test_full_flow_over_http(client):
    api, writer, settings, service = client

    writer.append("hello-1", "hello-2")
    assert api.post("/collect").json()["lines_inserted"] == 2

    rotation = api.post("/rotation/rotate").json()
    generation = rotation["generation"]

    # Rotation does not close the old handle: early delete is refused.
    assert api.post("/cleanup/delete", json={"generation": generation}).status_code == 409
    # And so is a second rotation.
    assert api.post("/rotation/rotate").status_code == 409

    writer.append("late")
    drained = api.post(f"/archives/{generation}/drain").json()
    assert drained["complete"] is True
    assert drained["final_size"] == rotation["size_at_rotate"] + len("late\n")

    entries = api.get("/entries", params={"generation": generation}).json()
    assert [i["content"] for i in entries["items"]] == [
        "hello-1",
        "hello-2",
        "late",
    ]

    deleted = api.post("/cleanup/delete", json={"generation": generation}).json()
    assert deleted["status"] == "deleted"
    assert api.get(f"/archives/{generation}").json()["status"] == "deleted"


def test_operation_id_must_match_generation(client):
    api, writer, settings, service = client
    writer.append("a")
    api.post("/collect")
    generation = api.post("/rotation/rotate").json()["generation"]
    writer.append("b")
    api.post(f"/archives/{generation}/drain")

    response = api.post(
        "/cleanup/delete", json={"generation": generation, "operation_id": 999}
    )
    assert response.status_code == 409
    # Nothing was deleted.
    assert service.archive(generation)["status"] == "ingested"


def test_retry_endpoint_resumes_interrupted_delete(client, monkeypatch):
    api, writer, settings, service = client
    writer.append("a")
    api.post("/collect")
    generation = api.post("/rotation/rotate").json()["generation"]
    api.post(f"/archives/{generation}/drain")

    import os

    real_unlink = os.unlink

    def boom(path):
        real_unlink(path)
        raise OSError("simulated crash")

    monkeypatch.setattr(os, "unlink", boom)
    with pytest.raises(OSError):
        api.post("/cleanup/delete", json={"generation": generation})
    monkeypatch.setattr(os, "unlink", real_unlink)

    status = api.get(f"/archives/{generation}").json()
    assert status["operation_status"] == "failed"

    retried = api.post(
        "/cleanup/retry", params={"generation": generation, "operation_id": status["operation_id"]}
    )
    assert retried.status_code == 200
    assert retried.json()["status"] == "deleted"
    assert api.get(f"/archives/{generation}").json()["status"] == "deleted"


def test_retry_with_unknown_operation_is_rejected(client):
    api, writer, settings, service = client
    writer.append("a")
    api.post("/collect")
    generation = api.post("/rotation/rotate").json()["generation"]
    writer.append("b")
    api.post(f"/archives/{generation}/drain")
    status = api.get(f"/archives/{generation}").json()
    # No operation exists yet.
    assert status["operation_id"] is None
    response = api.post(
        "/cleanup/retry", params={"generation": generation, "operation_id": 1}
    )
    assert response.status_code == 404


def test_openapi_documents_the_workflow(client):
    api, *_ = client
    schema = api.get("/openapi.json").json()
    paths = schema["paths"]
    for path in (
        "/collect",
        "/rotation/rotate",
        "/archives/{generation}/drain",
        "/cleanup/delete",
        "/cleanup/retry",
        "/entries",
        "/archives",
    ):
        assert path in paths, path
    tags = {tag for path in paths.values() for op in path.values() for tag in op.get("tags", [])}
    assert {"collection", "rotation", "cleanup", "inspection"} <= tags
