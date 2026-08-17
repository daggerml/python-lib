from datetime import datetime, timezone
from io import BytesIO

from daggerml._core.s3_cas import S3Remote


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
