import pytest

from daggerml.dashboard.cancellation import CancellationCoordinator


class _Runtime:
    def __init__(self):
        self.calls = []
        self.lifecycle = "running"

    def cancel(self, execution_id, *, mode):
        self.calls.append((execution_id, mode))
        if mode == "drive":
            self.lifecycle = "canceled"
        return {"cancelled": [], "inactive": [], "timeout": [], "error": []}

    def read_execution_record(self, execution_id):
        return {"execution_id": execution_id, "lifecycle": self.lifecycle}


class _Model:
    def __init__(self):
        self.dml = type("Dml", (), {"runtime": _Runtime()})()

    def execution(self, execution_id):
        return {"record": {"execution_id": execution_id}}


def test_dash_cancel_001__nonce_is_target_bound_one_use_and_full_is_planned_once():
    model = _Model()
    coordinator = CancellationCoordinator(model, interval=0, drive_timeout=1)
    nonce = coordinator.issue_nonce("one")["nonce"]

    coordinator.start("one", nonce)
    coordinator.drive("one")

    assert model.dml.runtime.calls == [("one", "full"), ("one", "drive")]
    with pytest.raises(PermissionError):
        coordinator.start("one", nonce)


def test_dash_cancel_002__nonce_cannot_cancel_another_execution():
    model = _Model()
    coordinator = CancellationCoordinator(model)
    nonce = coordinator.issue_nonce("one")["nonce"]

    with pytest.raises(PermissionError):
        coordinator.start("two", nonce)
