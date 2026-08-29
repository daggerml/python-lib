import pytest
from fastapi.testclient import TestClient

from daggerml._core.db import Ref
from daggerml._core.types import DmlRepoError
from daggerml.dashboard.read_model import DashboardReadModel, RevisionError
from daggerml.dashboard.server import create_app

pytest.importorskip("httpx")


COMMIT = "a" * 64


def test_dash_revision_001__head_and_concrete_commit_resolution_never_fall_back(tmp_path):
    (tmp_path / ".dml").mkdir()
    (tmp_path / ".dml" / "HEAD").write_text("", encoding="utf-8")

    class Dml:
        def status(self):
            return {"commit": f"commit:{COMMIT}"}

        def show(self, revision):
            if revision == f"commit:{COMMIT}":
                return {"id": COMMIT}
            raise DmlRepoError("missing")

    model = DashboardReadModel(tmp_path, dml_factory=lambda **_kwargs: Dml())

    assert model.resolve_revision("HEAD") == {
        "requested": "HEAD",
        "state": "ready",
        "commit": COMMIT,
        "current_head": COMMIT,
        "is_current_head": True,
    }
    assert model.resolve_revision(COMMIT)["is_current_head"] is True
    with pytest.raises(RevisionError, match="not available"):
        model.resolve_revision("b" * 64)
    with pytest.raises(RevisionError, match="must be HEAD"):
        model.resolve_revision("main")


def test_dash_revision_002__workspace_routes_require_registered_project_and_revision_scope(tmp_path, monkeypatch):
    project = tmp_path / "project"
    project.mkdir()
    (project / ".dml").mkdir()
    (project / ".dml" / "HEAD").write_text("", encoding="utf-8")
    app = create_app(tmp_path / "config")
    registered = app.state.projects.register(project)

    class Model:
        initialized = True

        def overview(self, revision):
            return {"revision": {"requested": revision}}

    app.state.project_models[str(project.resolve())] = Model()
    client = TestClient(app)
    headers = {"host": "127.0.0.1:8765"}

    missing = client.get("/api/v1/overview", headers=headers)
    unknown = client.get("/api/v1/overview?project=unknown&revision=HEAD", headers=headers)
    response = client.get(f"/api/v1/overview?project={registered['id']}&revision=HEAD", headers=headers)

    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "project-not-registered"
    assert unknown.status_code == 404
    assert unknown.json()["error"]["code"] == "project-not-registered"
    assert response.json() == {"revision": {"requested": "HEAD"}}


def test_dash_revision_003__workspace_detail_and_search_links_carry_explicit_scope(tmp_path):
    class Model:
        initialized = True

        def dag(self, dag_id, *, project, revision):
            return {"dag": dag_id, "project": project, "revision": revision}

        def search(self, query, *, limit, project, revision):
            return {
                "items": [
                    {
                        "kind": "dag",
                        "target": "dag:one",
                        "href": f"/projects/{project}/commits/{revision}",
                    }
                ]
            }

    app = create_app(tmp_path)

    def get_project(project):
        if project == "registered":
            return tmp_path
        raise DmlRepoError("no")

    app.state.projects.get = get_project
    app.state.project_models[str(tmp_path.resolve())] = Model()
    client = TestClient(app)
    headers = {"host": "127.0.0.1:8765"}

    dag = client.get(f"/api/v1/dags/dag:one?project=registered&revision={COMMIT}", headers=headers)
    search = client.get(f"/api/v1/search?q=one&project=registered&revision={COMMIT}", headers=headers)

    assert dag.json() == {"dag": "dag:one", "project": "registered", "revision": COMMIT}
    assert search.json()["items"][0] == {
        "kind": "dag",
        "target": "dag:one",
        "type": "dag",
        "id": "dag:one",
        "project_id": "registered",
        "href": f"/projects/registered/commits/{COMMIT}",
    }


def test_dash_revision_004__concrete_revision_remains_stable_after_head_moves(tmp_path):
    (tmp_path / ".dml").mkdir()
    (tmp_path / ".dml" / "HEAD").write_text("", encoding="utf-8")
    first = "a" * 64
    second = "b" * 64

    class Dml:
        head = first

        class Dag:
            @staticmethod
            def describe(_dag_ref):
                return {"nodes": [], "error": None}

        dag = Dag()

        def status(self):
            return {"commit": f"commit:{self.head}"}

        def show(self, revision):
            if revision in {f"commit:{first}", f"commit:{second}"}:
                commit = revision.removeprefix("commit:")
                return {"id": revision, "dags": {commit: f"dag:{commit}"}}
            raise DmlRepoError("missing")

    dml = Dml()
    model = DashboardReadModel(tmp_path, dml_factory=lambda **_kwargs: dml)

    selected = model.resolve_revision(first)
    dml.head = second

    assert selected["commit"] == first
    assert model.resolve_revision(first) == {
        "requested": first,
        "state": "ready",
        "commit": first,
        "current_head": second,
        "is_current_head": False,
    }
    assert model.dags(first) == {
        "items": [
            {
                "name": first,
                    "id": f"dag:{first}",
                    "commit": f"commit:{first}",
                    "tags": [],
                    "node_count": 0,
                "status": "ready",
            }
        ],
        "revision": {
            "requested": first,
            "state": "ready",
            "commit": first,
            "current_head": second,
            "is_current_head": False,
        },
        "live_dags_eligible": False,
    }


def test_dash_revision_004a__function_context_navigation_caches_revision_membership(tmp_path):
    (tmp_path / ".dml").mkdir()
    (tmp_path / ".dml" / "HEAD").write_text("", encoding="utf-8")
    root = Ref("dag:root")
    child = Ref("dag:child")
    root_node = Ref("node:root")
    child_node = Ref("node:child")
    descriptions = {root: 0, child: 0}

    class Dag:
        @staticmethod
        def describe(dag_ref):
            descriptions[dag_ref] += 1
            return {
                "id": dag_ref,
                "nodes": [root_node] if dag_ref == root else [child_node],
                "names": {},
                "tags": ["root.v1"] if dag_ref == root else ["function.v1"],
            }

        @staticmethod
        def describe_node(node_ref):
            if node_ref == root_node:
                return {"id": root_node, "type": "FnNode", "dag": child, "argv": []}
            return {"id": child_node, "type": "LiteralNode"}

        @staticmethod
        def get_node(_node_ref, *, recursive):
            return "value"

    class Dml:
        dag = Dag()

        @staticmethod
        def status():
            return {"commit": f"commit:{COMMIT}"}

        @staticmethod
        def show(revision):
            if revision == f"commit:{COMMIT}":
                return {"id": revision, "dags": {"root": root}}
            raise DmlRepoError("missing")

    model = DashboardReadModel(tmp_path, dml_factory=lambda **_kwargs: Dml())
    model.dag("root", revision=COMMIT)
    child_payload = model.dag("child", revision=COMMIT)
    child_descriptions = descriptions[child]

    payload = model.node("child", revision=COMMIT)

    assert payload["revision"]["commit"] == COMMIT
    assert child_payload["tags"] == ["function.v1"]
    assert descriptions[child] == child_descriptions


def test_dash_revision_004b__nested_function_context_nodes_are_reachable_in_the_revision(tmp_path):
    (tmp_path / ".dml").mkdir()
    (tmp_path / ".dml" / "HEAD").write_text("", encoding="utf-8")
    root = Ref("dag:root")
    outer = Ref("dag:outer")
    inner = Ref("dag:inner")
    root_node = Ref("node:root")
    outer_node = Ref("node:outer")
    inner_node = Ref("node:inner")

    class Dag:
        @staticmethod
        def describe(dag_ref):
            nodes = {root: [root_node], outer: [outer_node], inner: [inner_node]}
            return {"id": dag_ref, "nodes": nodes[dag_ref], "names": {}}

        @staticmethod
        def describe_node(node_ref):
            if node_ref == root_node:
                return {"id": root_node, "type": "FnNode", "dag": outer, "argv": []}
            if node_ref == outer_node:
                return {"id": outer_node, "type": "FnNode", "dag": inner, "argv": []}
            return {"id": inner_node, "type": "LiteralNode"}

        @staticmethod
        def get_node(_node_ref, *, recursive):
            return "value"

    class Dml:
        dag = Dag()

        @staticmethod
        def status():
            return {"commit": f"commit:{COMMIT}"}

        @staticmethod
        def show(revision):
            if revision == f"commit:{COMMIT}":
                return {"id": revision, "dags": {"root": root}}
            raise DmlRepoError("missing")

    model = DashboardReadModel(tmp_path, dml_factory=lambda **_kwargs: Dml())

    assert model.node(inner_node.to, revision=COMMIT)["revision"]["commit"] == COMMIT


def test_dash_revision_005__failure_matrix_is_stable_and_safe(tmp_path):
    app = create_app(tmp_path)
    client = TestClient(app)
    headers = {"host": "127.0.0.1:8765"}

    project = tmp_path / "project"
    project.mkdir()
    (project / ".dml").mkdir()
    (project / ".dml" / "HEAD").write_text("", encoding="utf-8")
    registered = app.state.projects.register(project)

    class Model:
        initialized = True

        def overview(self, revision):
            if revision == "main":
                raise RevisionError("invalid-revision", "Revision must be HEAD or a concrete commit ID")
            raise RevisionError("revision-not-found", "Revision is not available in this project")

        def dag(self, _dag_id, *, project, revision):
            raise RevisionError("resource-not-in-revision", "Resource is not available in this revision")

    app.state.project_models[str(project.resolve())] = Model()

    unavailable_project = tmp_path / "unavailable"
    unavailable_project.mkdir()
    unavailable = app.state.projects.register(unavailable_project)

    invalid = client.get(f"/api/v1/overview?project={registered['id']}&revision=main", headers=headers)
    missing = client.get(f"/api/v1/overview?project={registered['id']}&revision={'c' * 64}", headers=headers)
    resource = client.get(
        f"/api/v1/dags/dag:missing?project={registered['id']}&revision={'c' * 64}", headers=headers
    )
    unknown_project = client.get("/api/v1/overview?project=unknown&revision=HEAD", headers=headers)
    unavailable_response = client.get(f"/api/v1/overview?project={unavailable['id']}&revision=HEAD", headers=headers)

    assert (invalid.status_code, invalid.json()["error"]["code"]) == (400, "invalid-revision")
    assert (missing.status_code, missing.json()["error"]["code"]) == (404, "revision-not-found")
    assert (resource.status_code, resource.json()["error"]["code"]) == (404, "resource-not-in-revision")
    assert (unknown_project.status_code, unknown_project.json()["error"]["code"]) == (404, "project-not-registered")
    assert (
        unavailable_response.status_code,
        unavailable_response.json()["error"]["code"],
    ) == (503, "project-unavailable")
    assert unavailable_response.json()["error"]["retryable"] is True
    responses = (invalid, missing, resource, unknown_project, unavailable_response)
    assert all("Traceback" not in response.text for response in responses)


def test_dash_revision_006__revision_overview_is_bounded_read_only_and_redacted(tmp_path):
    (tmp_path / ".dml").mkdir()
    (tmp_path / ".dml" / "HEAD").write_text("", encoding="utf-8")
    calls = []

    class Config:
        @staticmethod
        def show():
            calls.append("config.show")
            return {
                "project_home": str(tmp_path),
                "default": {},
                "remote": {"root": "s3://bucket/project?token=secret", "access_key": "secret"},
            }

    class Runtime:
        @staticmethod
        def list():
            calls.append("runtime.list")
            return []

    class Dml:
        config = Config()
        runtime = Runtime()

        @staticmethod
        def status():
            calls.append("status")
            return {"commit": f"commit:{COMMIT}", "values": list(range(201)), "authorization": "secret"}

        @staticmethod
        def show(revision):
            calls.append(("show", revision))
            return {"id": revision}

        def fetch(self, *_args, **_kwargs):
            pytest.fail("revision reads must not fetch")

        def checkout(self, *_args, **_kwargs):
            pytest.fail("revision reads must not alter checkout state")

    model = DashboardReadModel(tmp_path, dml_factory=lambda **_kwargs: Dml())
    model.history = lambda *_args, **_kwargs: {"items": []}

    payload = model.overview(COMMIT)

    assert payload["revision"]["commit"] == COMMIT
    assert payload["repository"] == {"commit": {"id": f"commit:{COMMIT}"}, "recent_commits": []}
    assert payload["current"]["status"]["authorization"] == "<redacted>"
    assert payload["current"]["status"]["values"][-1] == {"truncated": True, "remaining": 1}
    assert payload["current"]["config"]["remote"] == {
        "root": "s3://bucket/project",
        "access_key": "<redacted>",
    }
    assert payload["current"]["checkout"] == {"mode": None, "branch": None, "state": "ready"}
    assert calls == [
        "status",
        ("show", f"commit:{COMMIT}"),
        "status",
        "config.show",
        "runtime.list",
        ("show", f"commit:{COMMIT}"),
    ]


def test_dash_revision_007__revision_workspace_reads_are_isolated_to_the_selected_project(tmp_path):
    app = create_app(tmp_path / "config")
    first = tmp_path / "first"
    second = tmp_path / "second"
    for project in (first, second):
        (project / ".dml").mkdir(parents=True)
        (project / ".dml" / "HEAD").write_text("", encoding="utf-8")
    first_registered = app.state.projects.register(first)
    second_registered = app.state.projects.register(second)
    calls = []

    class Model:
        initialized = True

        def __init__(self, project_id):
            self.project_id = project_id

        def overview(self, revision):
            calls.append((self.project_id, revision))
            return {"project": self.project_id, "revision": revision}

    app.state.project_models[str(first.resolve())] = Model(first_registered["id"])
    app.state.project_models[str(second.resolve())] = Model(second_registered["id"])

    response = TestClient(app).get(
        f"/api/v1/overview?project={first_registered['id']}&revision={COMMIT}",
        headers={"host": "127.0.0.1:8765"},
    )

    assert response.json() == {"project": first_registered["id"], "revision": COMMIT}
    assert calls == [(first_registered["id"], COMMIT)]


def test_dash_revision_008__dag_inventory_and_detail_keep_revision_scope_and_live_gate(tmp_path):
    project = tmp_path / "project"
    (project / ".dml").mkdir(parents=True)
    (project / ".dml" / "HEAD").write_text("", encoding="utf-8")
    app = create_app(tmp_path / "config")
    registered = app.state.projects.register(project)

    class Model:
        initialized = True

        def dags(self, revision):
            return {"items": [{"id": "dag:historical"}], "revision": {"commit": revision}, "live_dags_eligible": False}

        def dag(self, dag_id, *, project, revision):
            assert (dag_id, project, revision) == ("dag:historical", registered["id"], COMMIT)
            return {"id": dag_id, "revision": {"commit": revision}}

    app.state.project_models[str(project.resolve())] = Model()
    client = TestClient(app)
    headers = {"host": "127.0.0.1:8765"}

    inventory = client.get(f"/api/v1/dags?project={registered['id']}&revision={COMMIT}", headers=headers)
    detail = client.get(f"/api/v1/dags/dag:historical?project={registered['id']}&revision={COMMIT}", headers=headers)

    assert inventory.json()["live_dags_eligible"] is False
    assert inventory.json()["revision"]["commit"] == COMMIT
    assert detail.json()["revision"]["commit"] == COMMIT


def test_dash_revision_009__partial_dag_detail_is_available_only_at_current_head(tmp_path):
    (tmp_path / ".dml").mkdir()
    (tmp_path / ".dml" / "HEAD").write_text("", encoding="utf-8")
    current = "a" * 64
    historical = "b" * 64

    class Runtime:
        @staticmethod
        def list():
            return [{"dag": Ref("dag:partial")}]

    class Dag:
        @staticmethod
        def describe(_dag):
            return {"names": {}, "nodes": [], "argv": None}

    class Dml:
        runtime = Runtime()
        dag = Dag()

        @staticmethod
        def status():
            return {"commit": f"commit:{current}"}

        @staticmethod
        def show(revision):
            if revision in {f"commit:{current}", f"commit:{historical}"}:
                return {"id": revision, "dags": {}}
            raise DmlRepoError("missing")

    model = DashboardReadModel(tmp_path, dml_factory=lambda **_kwargs: Dml())

    assert model.dag("dag:partial", revision=current)["revision"]["is_current_head"] is True
    with pytest.raises(RevisionError, match="not available in this revision"):
        model.dag("dag:partial", revision=historical)
