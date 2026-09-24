from pathlib import Path


def test_tutorial_dockerfile_installs_a_minimal_runtime():
    repo_root = Path(__file__).resolve().parents[3]
    dockerfile = (repo_root / "docs/Dockerfile").read_text()
    builder, runtime = dockerfile.split("FROM python:3.13-alpine AS runtime")

    assert "apk add --no-cache build-base cmake" in builder
    assert "RUN rm -rf src/daggerml/dashboard" in builder
    assert "--wheel-dir /tmp/wheels" in builder
    assert "COPY --from=builder /install /usr/local" in runtime
    assert "apk add" not in runtime
    assert "COPY ." not in runtime
