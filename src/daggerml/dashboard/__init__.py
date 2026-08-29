"""Public custom-dashboard types and optional local dashboard support."""

from typing import TYPE_CHECKING

from daggerml.dashboard.models import Dashboard, PlotlyDashboardResult, VegaLiteDashboardResult

if TYPE_CHECKING:
    from daggerml.dashboard.read_model import DashboardReadModel as DashboardReadModel
    from daggerml.dashboard.serialization import bounded_json as bounded_json
    from daggerml.dashboard.serialization import project_runnable as project_runnable
    from daggerml.dashboard.serialization import redact as redact

__all__ = [
    "Dashboard",
    "DashboardReadModel",
    "PlotlyDashboardResult",
    "VegaLiteDashboardResult",
    "bounded_json",
    "project_runnable",
    "redact",
]


def __getattr__(name: str):
    """Load implementation helpers lazily to keep plugin imports lightweight."""
    if name == "DashboardReadModel":
        from daggerml.dashboard.read_model import DashboardReadModel

        return DashboardReadModel
    if name in {"bounded_json", "project_runnable", "redact"}:
        from daggerml.dashboard import serialization

        return getattr(serialization, name)
    raise AttributeError(name)
