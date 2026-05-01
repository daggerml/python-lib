import pytest

from daggerml._internal._db import Ref
from daggerml._internal.config import DmlProjectConfig, init_project_layout
from daggerml._internal.ops import DmlOps

pytestmark = pytest.mark.slow


def test_push_lifecycle_uses_configured_uri_and_optional_tag(tmp_path, aws_server):
    import boto3

    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    init_project_layout(
        repo_dir,
        DmlProjectConfig(name="demo", owner="alice", branch="main", remote_uri="s3://test-bucket/test-prefix"),
    )

    s3 = boto3.client("s3", endpoint_url=aws_server["endpoint"])
    try:
        s3.create_bucket(Bucket="test-bucket")
    except Exception:
        pass

    with DmlOps.create(str(repo_dir), remote_root="s3://test-bucket/test-prefix", branch="main"):
        pass

    with DmlOps.open(str(repo_dir), remote_root="s3://test-bucket/test-prefix") as ops:
        branch_ref_path = ops.push_project(None, head=Ref("head:main"), create=True, force=False, s3_client=s3)
        tag_ref_path = ops.push_project("v1.0", head=Ref("head:main"), create=False, force=False, s3_client=s3)

    assert branch_ref_path == "projects/alice/demo/heads/main.json"
    assert tag_ref_path == "projects/alice/demo/tags/v1.0.json"

    objects = s3.list_objects_v2(Bucket="test-bucket", Prefix="test-prefix/dml/refs/projects/alice/demo/")
    keys = {obj["Key"] for obj in objects.get("Contents", [])}
    assert "test-prefix/dml/refs/projects/alice/demo/heads/main.json" in keys
    assert "test-prefix/dml/refs/projects/alice/demo/tags/v1.0.json" in keys
