import pytest

from daggerml.dashboard.logs import read_cloudwatch_log


def test_dash_log_001__cloudwatch_rejects_noncanonical_streams():
    with pytest.raises(ValueError, match="stdout or stderr"):
        read_cloudwatch_log(object(), "cache", "local")


def test_dash_log_003__cloudwatch_uses_canonical_stream_and_hides_response_metadata():
    class Client:
        def get_log_events(self, **kwargs):
            assert kwargs["logGroupName"] == "dml"
            assert kwargs["logStreamName"] == "/run/cache/stdout"
            return {
                "events": [{"timestamp": 1, "message": "hello"}],
                "nextForwardToken": "next",
                "ResponseMetadata": {"HTTPHeaders": {"authorization": "secret"}},
            }

    result = read_cloudwatch_log(Client(), "cache", "stdout")

    assert result["events"] == [{"timestamp": 1, "message": "hello"}]
    assert "ResponseMetadata" not in result
