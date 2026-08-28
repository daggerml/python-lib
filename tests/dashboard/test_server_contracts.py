import pytest

pytest.importorskip("httpx")

from fastapi.testclient import TestClient

from daggerml.dashboard.read_model import DashboardReadModel
from daggerml.dashboard.server import create_app


class _Model:
    initialized = False

    def overview(self):
        return {"initialized": False, "diagnostic": "empty"}


def test_dash_http_001__overview_requires_registered_project_and_revision_scope(tmp_path):
    app = create_app(tmp_path)
    app.state.read_model = _Model()

    response = TestClient(app).get("/api/v1/overview", headers={"host": "127.0.0.1:8765"})

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "project-not-registered"
    assert response.headers["cache-control"] == "no-store"


def test_dash_http_002__projects_can_be_registered_without_mutating_the_project(tmp_path, monkeypatch):
    app = create_app(tmp_path)
    config_dir = tmp_path / "dashboard-config"
    monkeypatch.setattr(type(app.state.projects), "directory", property(lambda _self: config_dir))
    client = TestClient(app)
    project = tmp_path / "another-project"
    project.mkdir()

    response = client.post(
        "/api/v1/projects",
        json={"path": str(project), "name": "Another"},
        headers={"host": "127.0.0.1:8765"},
    )

    assert response.status_code == 201
    assert response.json()["name"] == "Another"
    assert client.get("/api/v1/projects", headers={"host": "127.0.0.1:8765"}).json()["items"]


def test_dash_http_003__overview_degrades_when_runtime_execution_state_is_unavailable(tmp_path):
    (tmp_path / ".dml").mkdir()
    (tmp_path / ".dml" / "HEAD").write_text("", encoding="utf-8")

    class Dml:
        def status(self):
            return {"branch": "main"}

        config = type(
            "Config",
            (),
            {
                "show": lambda _self: {
                    "project_home": str(tmp_path),
                    "default": {},
                "remote": {"root": "s3://lab/research"},
                }
            },
        )()

    model = DashboardReadModel(tmp_path, dml_factory=lambda **_kwargs: Dml())
    model.runtimes = lambda: {"items": [{"id": "index:root", "execution": None}]}
    model.history = lambda **_kwargs: {"items": []}

    assert model.overview()["active_jobs"] == 0
    assert "project_uri" not in model.overview()


def test_dash_sec_001__host_and_origin_checks_prevent_rebinding_and_csrf(tmp_path):
    client = TestClient(create_app(tmp_path))

    assert client.get("/api/v1/health", headers={"host": "evil.example"}).status_code == 403
    assert (
        client.get(
            "/api/v1/health",
            headers={"host": "127.0.0.1:8765", "origin": "https://evil.example"},
        ).status_code
        == 403
    )


def test_dash_sec_002__configured_bearer_token_protects_api(tmp_path):
    client = TestClient(create_app(tmp_path, auth_token="token"))

    assert client.get("/api/v1/health").status_code == 401
    assert client.get("/api/v1/status").status_code == 401
    assert client.get("/api/v1/health", headers={"authorization": "Bearer token"}).status_code == 200
    assert client.get("/api/v1/status", headers={"authorization": "Bearer token"}).status_code == 200
    assert client.get("/api/v1/health?token=token").status_code == 401
    assert client.get("/api/v1/not-found/events?token=token").status_code == 404


def test_dash_cancel_003__cancel_requires_json_content_type(tmp_path):
    client = TestClient(create_app(tmp_path))

    response = client.post(
        "/api/v1/executions/abc/cancel",
        content='{"mode":"full","nonce":"value"}',
        headers={"content-type": "text/plain", "host": "127.0.0.1:8765"},
    )

    assert response.status_code == 415
    assert response.json()["error"]["code"] == "invalid-content-type"


def test_dash_http_004__function_dag_script_route_uses_the_persisted_context(tmp_path):
    app = create_app(tmp_path)

    class Model:
        initialized = True

        def function_dag_script(self, dag_id, *, revision, max_bytes):
            assert dag_id == "dag:context"
            assert revision is None
            assert max_bytes == 4096
            return {"source": "def train():\n    return 1", "truncated": False}

    app.state.read_model = Model()

    response = TestClient(app).get(
        "/api/v1/function-dags/dag:context/script?max_bytes=4096",
        headers={"host": "127.0.0.1:8765"},
    )

    assert response.status_code == 200
    assert response.json()["source"].startswith("def train")


def test_dash_http_005__function_dag_log_route_uses_the_persisted_context(tmp_path):
    app = create_app(tmp_path)

    class Model:
        initialized = True

        def function_dag_logs(self, dag_id, stream, *, cursor, limit):
            assert (dag_id, stream, cursor, limit) == ("dag:context", "stdout", "next", 2048)
            return {"source": "cloudwatch", "events": [{"message": "done"}]}

    app.state.read_model = Model()

    response = TestClient(app).get(
        "/api/v1/function-dags/dag:context/logs/stdout?cursor=next&limit=2048",
        headers={"host": "127.0.0.1:8765"},
    )

    assert response.status_code == 200
    assert response.json()["events"] == [{"message": "done"}]


def test_dash_http_005a__node_value_script_route_preserves_project_and_revision_scope(tmp_path):
    project = tmp_path / "project"
    project.mkdir()
    (project / ".dml").mkdir()
    (project / ".dml" / "HEAD").write_text("", encoding="utf-8")
    app = create_app(tmp_path / "config")
    registered = app.state.projects.register(project)

    class Model:
        initialized = True

        def node_value_script(self, node_id, *, revision, max_bytes):
            assert (node_id, revision, max_bytes) == ("node-literal:value", "a" * 64, 2048)
            return {"source": "return 1", "truncated": False}

    app.state.project_models[str(project.resolve())] = Model()
    response = TestClient(app).get(
        f"/api/v1/nodes/node-literal:value/value/script?project={registered['id']}&revision={'a' * 64}&max_bytes=2048",
        headers={"host": "127.0.0.1:8765"},
    )

    assert response.status_code == 200
    assert response.json() == {"source": "return 1", "truncated": False}


def test_dash_sec_003__status_uses_only_registered_paths_and_returns_safe_diagnostics(tmp_path):
    app = create_app(tmp_path)

    class Status:
        def read(self, **options):
            assert options == {
                "project_cursor": None,
                "live_cursor": None,
                "commit_cursor": None,
                "limit": 50,
            }
            return {
                "generated_at": "2026-08-08T12:00:00Z",
                "retention_days": 365,
                "projects": {
                    "items": [{"id": "safe", "name": "Safe", "path": "/registered/safe"}],
                    "next_cursor": None,
                },
                "live_indexes": {"items": [], "next_cursor": None},
                "recent_commits": {"items": [], "next_cursor": None},
                "diagnostics": [
                    {
                        "project_id": "safe",
                        "availability": "unavailable",
                        "code": "project-read-failed",
                        "message": "Project state could not be read",
                        "retryable": True,
                    }
                ],
                "truncated": False,
            }

    app.state.status = Status()

    response = TestClient(app).get(
        "/api/v1/status?path=/untrusted/secret&remote=https://attacker.example",
        headers={"host": "127.0.0.1:8765"},
    )

    assert response.status_code == 200
    encoded = response.text
    assert "/registered/safe" in encoded
    assert "/untrusted/secret" not in encoded
    assert "attacker.example" not in encoded
    assert "Traceback" not in encoded
