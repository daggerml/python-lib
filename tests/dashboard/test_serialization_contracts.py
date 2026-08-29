from daggerml._core.db import Ref
from daggerml._core.types import Runnable, Uri
from daggerml.dashboard.serialization import bounded_json, project_runnable, redact


def test_dash_safe_001__redaction_removes_secrets_environment_and_query_strings():
    value = {
        "authorization": "Bearer secret",
        "environment": {"TOKEN": "secret"},
        "artifact": "s3://bucket/key?X-Amz-Credential=secret#frag",
        "nested": {"password": "secret", "visible": "ok"},
    }

    assert redact(value) == {
        "authorization": "<redacted>",
        "environment": "<redacted>",
        "artifact": "s3://bucket/key",
        "nested": {"password": "<redacted>", "visible": "ok"},
    }


def test_dash_safe_002__bounded_json_limits_strings_collections_and_depth():
    payload = bounded_json(
        {"long": "abcdefgh", "items": [1, 2, 3], "deep": {"a": {"b": 1}}},
        max_string=4,
        max_items=2,
        max_depth=2,
    )

    assert payload["long"] == {"text": "abcd", "truncated": True, "total_chars": 8}
    assert payload["_truncated"] == {"remaining": 1}


def test_dash_safe_003__refs_use_stable_wire_format():
    assert bounded_json(Ref("commit:abc123")) == "commit:abc123"


def test_dash_exec_001__runnable_projection_only_shows_pertinent_executor_fields():
    script = Runnable(
        target=Uri("script"),
        adapter="dml-local-adapter",
        kwargs={"fn_name": "train", "script_uri": "s3://bucket/code.py", "prepop": {"secret": "x"}},
    )
    docker = Runnable(
        target=Uri("docker"),
        adapter="dml-local-adapter",
        kwargs={"image": "repo/image:latest", "flags": ["--rm"], "environment": {"BAD": "x"}},
        sub=script,
    )

    projected = project_runnable(docker)

    assert projected["details"] == {"image": "repo/image:latest", "flags": ["--rm"]}
    assert projected["sub"]["kind"] == "script"
    assert projected["sub"]["details"] == {"fn_name": "train", "script_uri": "s3://bucket/code.py"}


def test_dash_exec_002__lambda_batch_and_cfn_targets_are_recognized():
    batch = project_runnable(
        {
            "target": "lambda:arn:aws:lambda:us-west-2:123:function:batch",
            "adapter": "dml-lambda-adapter",
            "kwargs": {"image": "image", "cpu": 2, "memory": 4096, "gpu": 0},
            "state": {"job_id": "job-1"},
        }
    )
    cfn = project_runnable({"target": "cfn", "adapter": "adapter", "kwargs": {"stack_name": "research"}})

    assert batch["kind"] == "batch"
    assert batch["details"]["job_id"] == "job-1"
    assert cfn["kind"] == "cloudformation"


def test_dash_exec_003__ssh_passes_nested_resume_state_to_batch():
    projected = project_runnable(
        {
            "target": "ssh",
            "adapter": "dml-local-adapter",
            "kwargs": {"host": "worker"},
            "state": {"job_id": "job-1", "job_definition": "definition-1"},
            "sub": {
                "target": "arn:aws:lambda:us-east-1:123:function:adapter",
                "adapter": "dml-lambda-adapter",
                "kwargs": {"image": "image", "cpu": 2, "memory": 4096, "gpu": 0},
            },
        }
    )

    assert projected["kind"] == "ssh"
    assert projected["sub"]["kind"] == "batch"
    assert projected["sub"]["details"]["job_id"] == "job-1"
