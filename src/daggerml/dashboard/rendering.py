"""Trusted in-process execution of plugin-defined DAG dashboards."""

from __future__ import annotations

import json
import math
import threading
from collections.abc import Mapping, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any

from daggerml._core import Dml, Ref
from daggerml.api import Dag
from daggerml.dashboard.cache import DashboardCacheIdentity, DashboardResultCache
from daggerml.dashboard.models import PlotlyDashboardResult, VegaLiteDashboardResult
from daggerml.dashboard.plugins import (
    DashboardPluginDiagnostic,
    RegisteredDashboard,
    compatible_dashboard_metadata,
    compatible_dashboards,
)


class CustomDashboardError(Exception):
    """Safe custom-dashboard error carrying a stable response code."""

    def __init__(self, code: str, message: str, *, status_code: int = 400):
        super().__init__(message)
        self.code = code
        self.status_code = status_code


def _json_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, str, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("Dashboard results cannot contain NaN or infinity")
        return value
    if isinstance(value, Mapping):
        if not all(isinstance(key, str) for key in value):
            raise TypeError("Dashboard result object keys must be strings")
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_json_value(item) for item in value]
    raise TypeError(f"Dashboard result contains unsupported {type(value).__name__}")


def serialize_dashboard_result(value: Any) -> dict[str, Any]:
    """Convert an exact public result variant to strict declarative JSON."""
    if isinstance(value, PlotlyDashboardResult):
        payload = {
            "kind": "plotly",
            "data": value.data,
            "layout": value.layout,
            "config": value.config,
        }
    elif isinstance(value, VegaLiteDashboardResult):
        payload = {"kind": "vega-lite", "spec": value.spec}
    else:
        raise TypeError("Render function must return a custom dashboard result type")
    normalized = _json_value(payload)
    json.dumps(normalized, allow_nan=False)
    return normalized


class CustomDashboardService:
    """Compatibility, caching, and bounded execution for custom dashboards."""

    def __init__(
        self,
        dashboards: Sequence[RegisteredDashboard],
        diagnostics: Sequence[DashboardPluginDiagnostic],
        cache: DashboardResultCache,
        *,
        max_workers: int = 2,
    ):
        self.dashboards = list(dashboards)
        self.diagnostics = list(diagnostics)
        self.cache = cache
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="dml-dashboard")
        self._inflight: dict[str, Future[dict[str, Any]]] = {}
        self._lock = threading.Lock()

    def metadata(self, tags: Sequence[str]) -> dict[str, object]:
        payload = compatible_dashboard_metadata(self.dashboards, tags)
        payload["diagnostics"] = [diagnostic.as_dict() for diagnostic in self.diagnostics]
        return payload

    def render(
        self,
        *,
        dml: Dml,
        dag_ref: Ref,
        tags: Sequence[str],
        name: str,
        refresh: bool = False,
    ) -> dict[str, Any]:
        registered = self._select(name, tags)
        identity = DashboardCacheIdentity(
            dashboard=name,
            dag_ref=dag_ref.to,
            distribution=registered.distribution,
            distribution_version=registered.distribution_version,
            cache_version=registered.definition.cache_version,
        )
        if not refresh:
            cached = self.cache.get(identity)
            if cached is not None:
                return {**cached, "cache_hit": True}
        with self._lock:
            future = self._inflight.get(identity.key)
            if future is None:
                future = self._executor.submit(self._compute, registered, identity, dml, dag_ref)
                self._inflight[identity.key] = future
        try:
            return {**future.result(), "cache_hit": False}
        except CustomDashboardError:
            raise
        except Exception as exc:
            raise CustomDashboardError(
                "dashboard-render-failed",
                "Custom dashboard rendering failed",
                status_code=500,
            ) from exc
        finally:
            with self._lock:
                if self._inflight.get(identity.key) is future and future.done():
                    self._inflight.pop(identity.key, None)

    def close(self) -> None:
        self._executor.shutdown(wait=False, cancel_futures=True)

    def _select(self, name: str, tags: Sequence[str]) -> RegisteredDashboard:
        matches = compatible_dashboards(self.dashboards, tags)
        selected = next((item for item in matches if item.definition.name == name), None)
        if selected is not None:
            return selected
        if any(item.definition.name == name for item in self.dashboards):
            raise CustomDashboardError(
                "dashboard-incompatible",
                "Dashboard is not compatible with this DAG",
                status_code=404,
            )
        raise CustomDashboardError("dashboard-not-found", "Dashboard is not registered", status_code=404)

    def _compute(
        self,
        registered: RegisteredDashboard,
        identity: DashboardCacheIdentity,
        dml: Dml,
        dag_ref: Ref,
    ) -> dict[str, Any]:
        try:
            result = registered.definition.render(Dag(dml=dml, ref=dag_ref))
            payload = serialize_dashboard_result(result)
            return self.cache.put(identity, payload)
        except (TypeError, ValueError) as exc:
            raise CustomDashboardError("invalid-dashboard-result", str(exc), status_code=422) from exc
        except CustomDashboardError:
            raise
        except Exception as exc:
            raise CustomDashboardError(
                "dashboard-render-failed",
                "Custom dashboard rendering failed",
                status_code=500,
            ) from exc
