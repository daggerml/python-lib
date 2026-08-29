from __future__ import annotations

from daggerml.util import BackoffWithJitter


def test_util_backoff_001__randomizes_initial_and_decorrelated_delays(monkeypatch):
    calls = []
    values = iter([150, 275])

    def fake_randint(low, high):
        calls.append((low, high))
        return next(values)

    monkeypatch.setattr("daggerml.util.randint", fake_randint)
    backoff = BackoffWithJitter()

    assert backoff() == 150
    assert backoff() == 275
    assert calls == [(100, 200), (100, 300)]


def test_util_backoff_002__caps_decorrelated_delay_at_ten_seconds(monkeypatch):
    monkeypatch.setattr("daggerml.util.randint", lambda low, high: 12_000)

    assert BackoffWithJitter(state=5_000)() == 10_000
