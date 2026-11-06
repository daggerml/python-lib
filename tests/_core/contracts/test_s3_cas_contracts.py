from datetime import datetime, timezone
from io import BytesIO

import pytest

from daggerml._core.s3_cas import CasItemConflict, S3Remote


def test_s3_cas_read_exposes_etag_last_modified_and_response_date() -> None:
    last_modified = datetime(2026, 8, 15, 12, 0, tzinfo=timezone.utc)

    class Client:
        def get_object(self, **kwargs):
            assert kwargs == {"Bucket": "bucket", "Key": "root/execution/e1.json"}
            return {
                "Body": BytesIO(b'{"execution_id":"e1"}'),
                "ETag": '"etag-1"',
                "LastModified": last_modified,
                "ResponseMetadata": {"HTTPHeaders": {"date": "Sat, 15 Aug 2026 12:00:05 GMT"}},
            }

    item = S3Remote("s3://bucket/root", Client())._get("root/execution/e1.json", cas=True)

    assert item.etag == "etag-1"
    assert item.last_modified == last_modified
    assert item.date == datetime(2026, 8, 15, 12, 0, 5, tzinfo=timezone.utc)


def test_s3_cas_put_normalizes_409_conditional_update_failure() -> None:
    class ConditionalUpdateFailed(Exception):
        response = {"Error": {"Code": "409"}}

    class Client:
        def put_object(self, **kwargs):
            raise ConditionalUpdateFailed()

    remote = S3Remote("s3://bucket/root", Client())

    with pytest.raises(CasItemConflict, match="was updated by another process"):
        remote._put("root/execution/e1.json", "next", overwrite=False)


def test_s3_transport_failure_with_null_response_is_not_masked() -> None:
    class Disconnected(ConnectionError):
        response = None

    class Client:
        def put_object(self, **kwargs):
            raise Disconnected("connection closed")

    remote = S3Remote("s3://bucket/root", Client())
    with pytest.raises(Disconnected, match="connection closed"):
        remote._put("root/pack", b"compressed pack")
    assert not remote._is_missing_error(Disconnected())


def test_binary_read_preserves_bytes_and_closes_body():
    body = BytesIO(b"\x00\xffcompressed bytes\n")

    class Client:
        def get_object(self, **kwargs):
            return {"Body": body}

    assert S3Remote("s3://bucket/root", Client())._get_bytes("root/pack") == b"\x00\xffcompressed bytes\n"
    assert body.closed
