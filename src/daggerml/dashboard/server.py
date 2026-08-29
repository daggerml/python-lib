"""FastAPI application and launcher for the local DaggerML dashboard."""

from __future__ import annotations

import asyncio
import json
import secrets
import threading
import webbrowser
from importlib.resources import files
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from daggerml._core import DmlRepoError, Ref
from daggerml.dashboard.cache import DashboardResultCache
from daggerml.dashboard.cancellation import CancellationCoordinator
from daggerml.dashboard.config import DashboardProjects
from daggerml.dashboard.plugins import load_dashboard_plugins
from daggerml.dashboard.read_model import DashboardReadModel, RevisionError, ScriptReadError
from daggerml.dashboard.rendering import CustomDashboardError, CustomDashboardService
from daggerml.dashboard.status import DashboardStatus, StatusCursorError


class ProjectScopeError(Exception):
    def __init__(self, code: str, message: str, *, retryable: bool = False):
        super().__init__(message)
        self.code = code
        self.retryable = retryable


def _require_fastapi():
    try:
        import fastapi
    except ImportError as exc:
        raise RuntimeError(
            "The dashboard dependencies are not installed. Install DaggerML with the 'dashboard' extra."
        ) from exc
    return fastapi


def create_app(config_home: str | Path | None = None, *, auth_token: str | None = None):
    """Create the local, versioned dashboard ASGI application.

    Parameters
    ----------
    config_home
        DaggerML configuration directory used to discover registered projects
        and the configured default project.
    auth_token
        Optional bearer token required on every API request. The launcher uses
        this for explicitly enabled non-loopback binding.
    """
    fastapi = _require_fastapi()
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
    from fastapi.staticfiles import StaticFiles

    app = fastapi.FastAPI(title="DaggerML Research Dashboard", version="1")
    app.state.projects = DashboardProjects(config_home)
    dashboard_plugins, dashboard_diagnostics = load_dashboard_plugins()
    app.state.custom_dashboards = CustomDashboardService(
        dashboard_plugins,
        dashboard_diagnostics,
        DashboardResultCache(app.state.projects.directory / "custom-dashboard-cache" / "v1"),
    )
    app.state.read_model = DashboardReadModel(app.state.projects.default_project)
    app.state.project_models = {}
    app.state.cancellation = CancellationCoordinator(app.state.read_model)
    app.state.project_cancellations = {}
    app.state.auth_token = auth_token
    app.router.add_event_handler("shutdown", lambda: app.state.custom_dashboards.close())
    roots_query = fastapi.Query(default=None)

    def error_response(status_code: int, code: str, message: str, *, retryable: bool = False):
        return JSONResponse(
            status_code=status_code,
            content={"error": {"code": code, "message": message, "retryable": retryable}},
        )

    # Same-origin is the intended deployment. Explicit CORS denial also keeps a
    # browser page on another origin from reading a loopback dashboard.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[],
        allow_credentials=False,
        allow_methods=[],
        allow_headers=[],
    )

    @app.middleware("http")
    async def security(request, call_next):
        host = request.headers.get("host", "")
        hostname = host.rsplit(":", 1)[0].strip("[]").lower()
        if auth_token is None and hostname not in {"127.0.0.1", "localhost", "::1"}:
            return error_response(403, "untrusted-host", "Untrusted Host header")
        origin = request.headers.get("origin")
        if auth_token is None and origin:
            parsed_origin = urlsplit(origin)
            origin_host = (parsed_origin.hostname or "").lower()
            origin_port = parsed_origin.port or (443 if parsed_origin.scheme == "https" else 80)
            request_port = request.url.port or (443 if request.url.scheme == "https" else 80)
            if origin_host != hostname or origin_port != request_port:
                return error_response(403, "cross-origin-denied", "Cross-origin request denied")
        if auth_token is not None and request.url.path.startswith("/api/"):
            header = request.headers.get("authorization", "")
            header_valid = secrets.compare_digest(header, f"Bearer {auth_token}")
            event_query_valid = (
                request.method == "GET"
                and request.url.path.endswith("/events")
                and secrets.compare_digest(request.query_params.get("token", ""), auth_token)
            )
            if not header_valid and not event_query_valid:
                return error_response(401, "invalid-token", "Invalid bearer token")
        if request.method == "POST" and (
            request.url.path.endswith("/cancel") or request.url.path.endswith("/dashboard/refresh")
        ):
            content_type = request.headers.get("content-type", "").partition(";")[0].strip().lower()
            if content_type != "application/json":
                return error_response(
                    415,
                    "invalid-content-type",
                    "This operation requires application/json",
                )
        response = await call_next(request)
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "no-referrer"
        return response

    @app.exception_handler(DmlRepoError)
    async def repository_error(_request, exc):
        return error_response(404, "repository-resource-not-found", str(exc))

    @app.exception_handler(RevisionError)
    async def revision_error(_request, exc):
        status_code = 400 if exc.code == "invalid-revision" else 404
        return error_response(status_code, exc.code, str(exc))

    @app.exception_handler(ScriptReadError)
    async def script_read_error(_request, exc):
        return error_response(exc.status_code, exc.code, str(exc))

    @app.exception_handler(ProjectScopeError)
    async def project_scope_error(_request, exc):
        status_code = 503 if exc.code == "project-unavailable" else 404
        return error_response(status_code, exc.code, str(exc), retryable=exc.retryable)

    @app.exception_handler(CustomDashboardError)
    async def custom_dashboard_error(_request, exc):
        return error_response(exc.status_code, exc.code, str(exc))

    @app.exception_handler(ValueError)
    async def value_error(_request, exc):
        return error_response(400, "invalid-request", str(exc))

    @app.exception_handler(StatusCursorError)
    async def status_cursor_error(_request, exc):
        return error_response(409, exc.code, str(exc), retryable=True)

    @app.exception_handler(fastapi.HTTPException)
    async def http_error(_request, exc):
        code = {
            400: "invalid-request",
            401: "invalid-token",
            403: "forbidden",
            404: "resource-not-found",
            409: "conflict",
            415: "invalid-content-type",
        }.get(exc.status_code, "request-failed")
        return error_response(exc.status_code, code, str(exc.detail))

    def model(project: str | None = None) -> DashboardReadModel:
        if project is None:
            return app.state.read_model
        project_home = app.state.projects.get(project)
        if project_home == app.state.read_model.project_home:
            return app.state.read_model
        key = str(project_home)
        if key not in app.state.project_models:
            app.state.project_models[key] = DashboardReadModel(project_home)
        return app.state.project_models[key]

    def scoped_model(project: str | None) -> DashboardReadModel:
        if not project:
            raise ProjectScopeError("project-not-registered", "Project is required")
        try:
            selected = model(project)
        except DmlRepoError as exc:
            raise ProjectScopeError("project-not-registered", "Project is not registered") from exc
        if not selected.initialized:
            raise ProjectScopeError("project-unavailable", "Project is unavailable", retryable=True)
        return selected

    def cancellation(project: str | None = None) -> CancellationCoordinator:
        selected = model(project)
        if selected is app.state.read_model:
            return app.state.cancellation
        key = str(selected.project_home)
        if key not in app.state.project_cancellations:
            app.state.project_cancellations[key] = CancellationCoordinator(selected)
        return app.state.project_cancellations[key]

    app.state.status = DashboardStatus(app.state.projects, lambda project_id: model(project_id))

    @app.get("/api/v1/projects")
    def projects():
        payload = app.state.projects.list()
        summaries = app.state.status.project_summaries()
        return {
            **payload,
            "items": [{**item, **summaries.get(item["id"], {})} for item in payload["items"]],
        }

    @app.get("/api/v1/status")
    def status(
        project_cursor: str | None = None,
        live_cursor: str | None = None,
        commit_cursor: str | None = None,
        limit: int = 50,
    ):
        return app.state.status.read(
            project_cursor=project_cursor,
            live_cursor=live_cursor,
            commit_cursor=commit_cursor,
            limit=limit,
        )

    @app.post("/api/v1/projects", status_code=201)
    def register_project(body: dict[str, Any]):
        path = body.get("path")
        name = body.get("name")
        if not isinstance(path, str):
            raise fastapi.HTTPException(status_code=400, detail="Project registration requires a path")
        if name is not None and not isinstance(name, str):
            raise fastapi.HTTPException(status_code=400, detail="Project name must be a string")
        had_default = app.state.projects.default_project is not None
        item = app.state.projects.register(path, name=name)
        if not had_default:
            app.state.read_model = DashboardReadModel(app.state.projects.default_project)
            app.state.cancellation = CancellationCoordinator(app.state.read_model)
        return item

    @app.delete("/api/v1/projects/{project_id}", status_code=204)
    def unregister_project(project_id: str):
        if not app.state.projects.unregister(project_id):
            raise fastapi.HTTPException(status_code=404, detail="Dashboard project is not registered")

    @app.get("/api/v1/health")
    def health(project: str | None = None):
        return {"ok": True, "initialized": model(project).initialized}

    @app.get("/api/v1/dml")
    def dml_api(project: str | None = None):
        return model(project).dml_api()

    @app.get("/api/v1/dml/runtimes/{runtime_id:path}")
    def dml_runtime(runtime_id: str, project: str | None = None):
        return model(project).dml_runtime(runtime_id)

    @app.get("/api/v1/overview")
    def overview(project: str | None = None, revision: str = ""):
        return scoped_model(project).overview(revision)

    @app.get("/api/v1/refs")
    def refs(project: str | None = None, revision: str = "", live: bool = True):
        return scoped_model(project).refs(revision, live=live)

    @app.get("/api/v1/history")
    def history(revision: str = "", cursor: str | None = None, limit: int = 50, project: str | None = None):
        return scoped_model(project).history(revision, cursor=cursor, limit=limit)

    @app.get("/api/v1/commits")
    def commits(revision: str = "", cursor: str | None = None, limit: int = 50, project: str | None = None):
        return scoped_model(project).history(revision, cursor=cursor, limit=limit)

    @app.get("/api/v1/commits/{revision:path}/diff")
    def commit_diff(revision: str, relative_to: str | None = None, project: str | None = None):
        return model(project).commit_diff(revision, relative_to)

    @app.get("/api/v1/commits/{revision:path}")
    def commit(revision: str, project: str | None = None):
        selected = scoped_model(project)
        scope = selected.resolve_revision(revision)
        if scope["state"] == "unborn":
            return {"revision": scope}
        return {"revision": scope, "repository": {"commit": selected.commit(scope["commit"])}}

    def custom_dashboard_context(dag_id: str, project: str | None, revision: str):
        selected = scoped_model(project)
        dag_payload = selected.dag(dag_id, project=project, revision=revision)
        tags = dag_payload.get("tags")
        return selected, Ref(str(dag_payload["id"])), [str(tag) for tag in tags] if isinstance(tags, list) else []

    @app.get("/api/v1/dags/{dag_id}/dashboards")
    def custom_dashboards(dag_id: str, project: str | None = None, revision: str = ""):
        _selected, _dag_ref, tags = custom_dashboard_context(dag_id, project, revision)
        return app.state.custom_dashboards.metadata(tags)

    @app.get("/api/v1/dags/{dag_id}/dashboard")
    def custom_dashboard(dag_id: str, name: str, project: str | None = None, revision: str = ""):
        selected, dag_ref, tags = custom_dashboard_context(dag_id, project, revision)
        return app.state.custom_dashboards.render(dml=selected.dml, dag_ref=dag_ref, tags=tags, name=name)

    @app.post("/api/v1/dags/{dag_id}/dashboard/refresh")
    def refresh_custom_dashboard(
        dag_id: str,
        body: dict[str, Any],
        project: str | None = None,
        revision: str = "",
    ):
        name = body.get("name")
        if not isinstance(name, str) or not name:
            raise ValueError("Custom dashboard refresh requires a name")
        selected, dag_ref, tags = custom_dashboard_context(dag_id, project, revision)
        return app.state.custom_dashboards.render(
            dml=selected.dml,
            dag_ref=dag_ref,
            tags=tags,
            name=name,
            refresh=True,
        )

    @app.get("/api/v1/dags/{dag_id:path}")
    def dag(dag_id: str, project: str | None = None, revision: str = ""):
        return scoped_model(project).dag(dag_id, project=project, revision=revision)

    @app.get("/api/v1/dags")
    def dags(revision: str = "", project: str | None = None):
        return scoped_model(project).dags(revision)

    @app.get("/api/v1/nodes/{node_id:path}/value/script")
    def node_value_script(
        node_id: str,
        max_bytes: int = 128 * 1024,
        project: str | None = None,
        revision: str = "",
    ):
        return scoped_model(project).node_value_script(node_id, revision=revision, max_bytes=max_bytes)

    @app.get("/api/v1/nodes/{node_id:path}")
    def node(node_id: str, recursive: bool = False, project: str | None = None, revision: str = ""):
        return scoped_model(project).node(node_id, recursive=recursive, project=project, revision=revision)

    @app.get("/api/v1/runtimes")
    def runtimes(project: str | None = None):
        return model(project).runtimes()

    @app.get("/api/v1/live-indexes/{index_id:path}")
    def live_index(index_id: str, project: str | None = None):
        return model(project).live_index(index_id)

    @app.get("/api/v1/executions")
    def executions(project: str | None = None):
        return model(project).runtimes()

    @app.get("/api/v1/search")
    def search(q: str, limit: int = 25, project: str | None = None, revision: str = ""):
        def client_item(item: dict[str, Any], project_id: str, project_name: str | None = None):
            result = {
                **item,
                "type": str(item.get("kind") or "resource"),
                "id": str(item.get("target") or ""),
                "project_id": project_id,
            }
            if project_name is not None:
                result["project_name"] = project_name
            return result

        if project is not None:
            payload = scoped_model(project).search(q, limit=limit, project=project, revision=revision)
            return {**payload, "items": [client_item(item, project) for item in payload["items"]]}
        items = []
        for registered in app.state.projects.list()["items"]:
            try:
                for item in model(registered["id"]).search(
                    q,
                    limit=limit,
                    project=registered["id"],
                    revision="HEAD",
                )["items"]:
                    items.append(client_item(item, registered["id"], registered["name"]))
            except Exception:
                continue
        return {"items": items[: max(1, min(int(limit), 100))]}

    @app.get("/api/v1/executions/graph")
    def execution_graph(root: list[str] | None = roots_query, project: str | None = None):
        return model(project).execution_graph(*(root or ()))

    @app.get("/api/v1/executions/{execution_id}")
    def execution(execution_id: str, project: str | None = None):
        return model(project).execution(execution_id)

    @app.get("/api/v1/fndags/{execution_id}")
    def fndag(execution_id: str, project: str | None = None):
        return model(project).fndag(execution_id)

    @app.get("/api/v1/executions/{execution_id}/logs/{stream}")
    def logs(
        execution_id: str,
        stream: str,
        cursor: str | None = None,
        limit: int = 64 * 1024,
        project: str | None = None,
    ):
        try:
            return model(project).logs(execution_id, stream, cursor=cursor, limit=limit)
        except FileNotFoundError as exc:
            raise fastapi.HTTPException(status_code=404, detail=str(exc)) from exc
        except PermissionError as exc:
            raise fastapi.HTTPException(status_code=403, detail=str(exc)) from exc

    @app.get("/api/v1/executions/{execution_id}/logs")
    def default_logs(
        execution_id: str,
        stream: str = "stdout",
        cursor: str | None = None,
        limit: int = 64 * 1024,
        project: str | None = None,
    ):
        return logs(execution_id, stream, cursor, limit, project)

    @app.get("/api/v1/executions/{execution_id}/script")
    def script(execution_id: str, max_bytes: int = 128 * 1024, project: str | None = None):
        try:
            return model(project).script(execution_id, max_bytes=max_bytes)
        except FileNotFoundError as exc:
            raise fastapi.HTTPException(status_code=404, detail=str(exc)) from exc
        except PermissionError as exc:
            raise fastapi.HTTPException(status_code=403, detail=str(exc)) from exc

    @app.get("/api/v1/function-dags/{dag_id:path}/script")
    def function_dag_script(
        dag_id: str,
        max_bytes: int = 128 * 1024,
        project: str | None = None,
        revision: str | None = None,
    ):
        try:
            return model(project).function_dag_script(dag_id, revision=revision, max_bytes=max_bytes)
        except FileNotFoundError as exc:
            raise fastapi.HTTPException(status_code=404, detail=str(exc)) from exc
        except PermissionError as exc:
            raise fastapi.HTTPException(status_code=403, detail=str(exc)) from exc

    @app.get("/api/v1/function-dags/{dag_id}/logs/{stream}")
    def function_dag_logs(
        dag_id: str,
        stream: str,
        cursor: str | None = None,
        limit: int = 64 * 1024,
        project: str | None = None,
    ):
        try:
            return model(project).function_dag_logs(dag_id, stream, cursor=cursor, limit=limit)
        except FileNotFoundError as exc:
            raise fastapi.HTTPException(status_code=404, detail=str(exc)) from exc
        except PermissionError as exc:
            raise fastapi.HTTPException(status_code=403, detail=str(exc)) from exc

    @app.get("/api/v1/remotes")
    def remotes(live: bool = True, project: str | None = None):
        return model(project).remotes(live=live)

    @app.post("/api/v1/executions/{execution_id}/cancel-confirmation")
    def cancellation_confirmation(execution_id: str, project: str | None = None):
        return cancellation(project).issue_nonce(execution_id)

    @app.post("/api/v1/executions/{execution_id}/cancel/nonce")
    def cancellation_nonce(execution_id: str, project: str | None = None):
        return cancellation(project).issue_nonce(execution_id)

    @app.post("/api/v1/executions/{execution_id}/cancel", status_code=202)
    def cancel(execution_id: str, body: dict[str, Any], project: str | None = None):
        if body.get("mode") != "full":
            raise fastapi.HTTPException(status_code=400, detail="Dashboard cancellation mode must be 'full'")
        nonce = body.get("nonce")
        if not isinstance(nonce, str):
            raise fastapi.HTTPException(status_code=400, detail="A confirmation nonce is required")
        try:
            coordinator = cancellation(project)
            summary = coordinator.start(execution_id, nonce)
        except PermissionError as exc:
            raise fastapi.HTTPException(status_code=409, detail=str(exc)) from exc
        threading.Thread(
            target=coordinator.drive,
            args=(execution_id,),
            daemon=True,
            name=f"dml-dashboard-cancel-{execution_id[:8]}",
        ).start()
        return {"accepted": True, "execution_id": execution_id, "summary": summary}

    async def event_stream(execution_id: str | None = None, project: str | None = None):
        previous = ""
        while True:
            try:
                payload = model(project).execution(execution_id) if execution_id else model(project).overview()
                encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
                if encoded != previous:
                    event = "repository"
                    if execution_id:
                        lifecycle = payload.get("record", {}).get("lifecycle")
                        event = (
                            "cancellation"
                            if lifecycle in {"cancel-requested", "cancel-ready", "canceled"}
                            else "execution"
                        )
                    yield f"event: {event}\ndata: {encoded}\n\n"
                    previous = encoded
                else:
                    yield ": heartbeat\n\n"
            except asyncio.CancelledError:
                return
            except Exception as exc:
                yield f"event: diagnostic\ndata: {json.dumps({'detail': str(exc)})}\n\n"
            await asyncio.sleep(1)

    @app.get("/api/v1/events")
    def events(project: str | None = None):
        return StreamingResponse(event_stream(project=project), media_type="text/event-stream")

    @app.get("/api/v1/executions/{execution_id}/events")
    def execution_events(execution_id: str, project: str | None = None):
        return StreamingResponse(event_stream(execution_id, project), media_type="text/event-stream")

    async def log_event_stream(
        resource_id: str,
        stream: str,
        project: str | None = None,
        *,
        function_dag: bool = False,
    ):
        cursor: str | None = None
        while True:
            try:
                reader = model(project).function_dag_logs if function_dag else model(project).logs
                payload = reader(resource_id, stream, cursor=cursor)
                cursor = str(payload.get("next_cursor") or cursor or "")
                if payload.get("text") or payload.get("events"):
                    yield f"event: log\ndata: {json.dumps(payload, separators=(',', ':'))}\n\n"
                else:
                    yield ": heartbeat\n\n"
            except asyncio.CancelledError:
                return
            except Exception as exc:
                yield f"event: diagnostic\ndata: {json.dumps({'detail': str(exc)})}\n\n"
            await asyncio.sleep(1)

    @app.get("/api/v1/executions/{execution_id}/logs/{stream}/events")
    def log_events(execution_id: str, stream: str, project: str | None = None):
        return StreamingResponse(log_event_stream(execution_id, stream, project), media_type="text/event-stream")

    @app.get("/api/v1/function-dags/{dag_id}/logs/{stream}/events")
    def function_dag_log_events(dag_id: str, stream: str, project: str | None = None):
        return StreamingResponse(
            log_event_stream(dag_id, stream, project, function_dag=True),
            media_type="text/event-stream",
        )

    try:
        static_root = Path(str(files("daggerml.dashboard").joinpath("static")))
    except (ModuleNotFoundError, TypeError):
        static_root = Path()
    if static_root.is_dir() and (static_root / "index.html").is_file():
        app.mount("/assets", StaticFiles(directory=static_root / "assets"), name="dashboard-assets")

        @app.get("/{path:path}", include_in_schema=False)
        def spa(path: str):
            if path == "api" or path.startswith("api/"):
                raise fastapi.HTTPException(status_code=404, detail="API route not found")
            candidate = (static_root / path).resolve()
            if path and static_root in candidate.parents and candidate.is_file():
                return FileResponse(candidate)
            return FileResponse(static_root / "index.html")
    else:

        @app.get("/", include_in_schema=False)
        def no_assets():
            return JSONResponse(
                status_code=503,
                content={
                    "detail": "Dashboard frontend assets are not installed.",
                    "api": "/api/v1/overview",
                },
            )
    return app


def run_dashboard(
    config_home: str | Path | None = None,
    *,
    host: str = "127.0.0.1",
    port: int = 8765,
    open_browser: bool = True,
    auth_token: str | None = None,
) -> None:
    """Run the local dashboard with Uvicorn."""
    _require_fastapi()
    try:
        import uvicorn
    except ImportError as exc:
        raise RuntimeError(
            "The dashboard dependencies are not installed. Install DaggerML with the 'dashboard' extra."
        ) from exc
    app = create_app(config_home=config_home, auth_token=auth_token)
    url = f"http://{host}:{port}/"
    if open_browser:
        threading.Timer(0.5, webbrowser.open, args=(url,)).start()
    uvicorn.run(app, host=host, port=port)


__all__ = ["create_app", "run_dashboard"]
