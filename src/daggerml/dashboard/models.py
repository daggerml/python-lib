"""Public value types for plugin-defined DAG dashboards."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, TypeAlias

from daggerml.api import Dag


@dataclass(frozen=True)
class PlotlyDashboardResult:
    """Declarative Plotly payload returned by a custom dashboard."""

    data: list[Mapping[str, Any]]
    layout: Mapping[str, Any] = field(default_factory=dict)
    config: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.data, list) or not all(isinstance(item, Mapping) for item in self.data):
            raise TypeError("PlotlyDashboardResult.data must be a list of mappings")
        if not isinstance(self.layout, Mapping):
            raise TypeError("PlotlyDashboardResult.layout must be a mapping")
        if not isinstance(self.config, Mapping):
            raise TypeError("PlotlyDashboardResult.config must be a mapping")


@dataclass(frozen=True)
class VegaLiteDashboardResult:
    """Declarative Vega-Lite payload returned by a custom dashboard."""

    spec: Mapping[str, Any]

    def __post_init__(self) -> None:
        if not isinstance(self.spec, Mapping):
            raise TypeError("VegaLiteDashboardResult.spec must be a mapping")


DashboardResult: TypeAlias = PlotlyDashboardResult | VegaLiteDashboardResult
DashboardRender: TypeAlias = Callable[[Dag], DashboardResult]


@dataclass(frozen=True)
class Dashboard:
    """Definition contributed by a ``daggerml.dashboards`` plugin provider."""

    name: str
    render: DashboardRender
    tags: frozenset[str] = field(default_factory=frozenset)
    eager: bool = False
    cache_version: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name or self.name != self.name.strip():
            raise TypeError("Dashboard.name must be a non-empty trimmed string")
        if not callable(self.render):
            raise TypeError("Dashboard.render must be callable")
        try:
            normalized_tags = frozenset(self.tags)
        except TypeError as exc:
            raise TypeError("Dashboard.tags must be an iterable of strings") from exc
        if not all(isinstance(tag, str) for tag in normalized_tags):
            raise TypeError("Dashboard.tags must contain only strings")
        if not isinstance(self.eager, bool):
            raise TypeError("Dashboard.eager must be a bool")
        if not isinstance(self.cache_version, str):
            raise TypeError("Dashboard.cache_version must be a string")
        object.__setattr__(self, "tags", normalized_tags)
