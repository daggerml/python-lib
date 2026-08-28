"""Discovery for installed custom DAG dashboard plugins."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from importlib import metadata

from daggerml.dashboard.models import Dashboard

DASHBOARD_ENTRYPOINT_GROUP = "daggerml.dashboards"


@dataclass(frozen=True)
class RegisteredDashboard:
    """A dashboard definition and the distribution identity that supplied it."""

    definition: Dashboard
    distribution: str
    distribution_version: str
    entry_point: str


@dataclass(frozen=True)
class DashboardPluginDiagnostic:
    """Bounded plugin discovery diagnostic safe for dashboard responses."""

    entry_point: str
    code: str
    message: str

    def as_dict(self) -> dict[str, str]:
        return {"entry_point": self.entry_point, "code": self.code, "message": self.message[:500]}


def _entry_points() -> list[metadata.EntryPoint]:
    points = metadata.entry_points()
    selected = points.select(group=DASHBOARD_ENTRYPOINT_GROUP)
    return sorted(selected, key=lambda point: (point.name, point.value))


def _distribution(point: metadata.EntryPoint) -> tuple[str, str]:
    distribution = getattr(point, "dist", None)
    if distribution is None:
        return "", ""
    name = str(distribution.metadata.get("Name") or "")
    return name, str(distribution.version or "")


def load_dashboard_plugins(
    points: Sequence[metadata.EntryPoint] | None = None,
) -> tuple[list[RegisteredDashboard], list[DashboardPluginDiagnostic]]:
    """Load installed dashboard providers while isolating invalid plugins."""
    registered: list[RegisteredDashboard] = []
    diagnostics: list[DashboardPluginDiagnostic] = []
    names: set[str] = set()
    selected = sorted(points, key=lambda point: (point.name, point.value)) if points is not None else _entry_points()
    for point in selected:
        label = f"{point.name} ({point.value})"
        try:
            provider = point.load()
            if not callable(provider):
                raise TypeError("entry point must load a zero-argument provider")
            definitions = provider()
            if isinstance(definitions, (str, bytes)) or not isinstance(definitions, Iterable):
                raise TypeError("provider must return an iterable of Dashboard values")
            distribution, version = _distribution(point)
            for definition in definitions:
                if not isinstance(definition, Dashboard):
                    diagnostics.append(
                        DashboardPluginDiagnostic(
                            label,
                            "invalid-definition",
                            "Provider returned a non-Dashboard value",
                        )
                    )
                    continue
                if definition.name in names:
                    diagnostics.append(
                        DashboardPluginDiagnostic(
                            label,
                            "duplicate-name",
                            f"Dashboard name '{definition.name}' is already registered",
                        )
                    )
                    continue
                names.add(definition.name)
                registered.append(RegisteredDashboard(definition, distribution, version, label))
        except Exception as exc:
            diagnostics.append(DashboardPluginDiagnostic(label, "plugin-load-failed", str(exc)))
    return registered, diagnostics


def compatible_dashboards(
    dashboards: Sequence[RegisteredDashboard], tags: Iterable[str]
) -> list[RegisteredDashboard]:
    """Return definitions whose exact required tags are present on a DAG."""
    dag_tags = frozenset(tags)
    return [item for item in dashboards if item.definition.tags <= dag_tags]


def compatible_dashboard_metadata(
    dashboards: Sequence[RegisteredDashboard], tags: Iterable[str]
) -> dict[str, object]:
    """Project compatible definitions and the first eager default."""
    compatible = compatible_dashboards(dashboards, tags)
    items = [
        {
            "name": item.definition.name,
            "tags": sorted(item.definition.tags),
            "eager": item.definition.eager,
        }
        for item in compatible
    ]
    default = next((item.definition.name for item in compatible if item.definition.eager), None)
    return {"items": items, "next_cursor": None, "default": default}
