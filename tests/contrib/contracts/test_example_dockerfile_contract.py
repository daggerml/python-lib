from pathlib import Path


def test_contrib_examples_003__docker_runtime_excludes_build_context_and_toolchain():
    repo_root = Path(__file__).resolve().parents[3]
    dockerfile = (repo_root / "examples/dkr-ctx/Dockerfile").read_text()
    builder, runtime = dockerfile.split("FROM python:3.13-slim AS runtime")

    assert "apt-get install" in builder
    assert "COPY . /app" in builder
    assert "python -m venv /opt/venv" in builder
    assert "COPY --from=builder /opt/venv /opt/venv" in runtime
    assert "apt-get" not in runtime
    assert "COPY ." not in runtime
