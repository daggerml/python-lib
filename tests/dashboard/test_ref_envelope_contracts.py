from daggerml._core import Ref
from daggerml.dashboard.read_model import DashboardReadModel

LOCAL = "a" * 64
LIVE = "b" * 64


def _model(tmp_path):
    (tmp_path / ".dml").mkdir()
    (tmp_path / ".dml" / "HEAD").write_text("", encoding="utf-8")

    class Branch:
        @staticmethod
        def list(*, remote=False, dep=None):
            if dep:
                return [{"name": "models", "commit": Ref(f"commit:{LIVE}")}]
            return [{"name": "main", "commit": Ref(f"commit:{LIVE if remote else LOCAL}")}]

        @staticmethod
        def get_upstream(name):
            return {"branch": "main"} if name == "main" else None

    class Tag:
        @staticmethod
        def list(*, remote=False, dep=None):
            return [{"name": "v1", "commit": Ref(f"commit:{LOCAL}")}]

    class Dep:
        @staticmethod
        def list():
            return {"models": "s3://bucket/dependency?credential=secret"}

    class Dml:
        branch = Branch()
        tag = Tag()
        dep = Dep()

        @staticmethod
        def status():
            return {"mode": "attached", "branch": "main", "commit": Ref(f"commit:{LOCAL}")}

        @staticmethod
        def show(revision):
            return {"id": revision}

    return DashboardReadModel(tmp_path, dml_factory=lambda **_kwargs: Dml())


def test_dash_refs_001__groups_public_local_and_live_ref_sources(tmp_path):
    payload = _model(tmp_path).refs(LOCAL)

    assert payload["checkout"] == {"mode": "attached", "branch": "main", "state": "ready"}
    assert payload["branches"] == [
        {
            "kind": "branch",
            "name": "main",
            "local": {"commit": f"commit:{LOCAL}", "inspectable": True},
            "live": {"commit": f"commit:{LIVE}", "inspectable": True},
            "upstream": "main",
            "relation": "unknown",
        }
    ]
    assert payload["tags"] == [
        {
            "kind": "tag",
            "name": "v1",
            "local": {"commit": f"commit:{LOCAL}", "inspectable": True},
            "live": {"commit": f"commit:{LOCAL}", "inspectable": True},
            "relation": "matching",
        }
    ]


def test_dash_refs_002__dependency_ref_reads_use_public_source_selectors(tmp_path):
    payload = _model(tmp_path).refs(LOCAL)

    dependency = payload["dependencies"]["items"][0]
    assert dependency["root"] == "s3://bucket/dependency"
    assert dependency["branches"][0]["fetched"]["commit"] == f"commit:{LIVE}"
    assert dependency["branches"][0]["live"]["commit"] == f"commit:{LIVE}"
