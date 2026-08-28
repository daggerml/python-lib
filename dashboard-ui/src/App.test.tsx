// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import App from "./App";
import { api } from "./api";
import type { DashboardProject } from "./types";

const { projects, eventHandlers, plotlyNew, plotlyPurge, vegaEmbed } = vi.hoisted(() => ({
  projects: [
    { id: "project-1", name: "research", path: "/workspace/research", local_available: true, path_context: { parent: "/workspace", leaf: "research" }, live_index_count: 2, availability: "complete", checkout: { branch: "main" }, sync: { state: "in-sync" }, last_activity: { state: "known", timestamp: "2026-01-01T00:00:00Z" } },
    { id: "project-2", name: "research", path: "/archive/research", local_available: false, path_context: { parent: "/archive", leaf: "research" }, live_index_count: 0, availability: "unavailable", checkout: { state: "unavailable" }, sync: { state: "unknown" }, last_activity: { state: "unavailable" } },
  ] satisfies DashboardProject[],
  eventHandlers: [] as Array<(event: MessageEvent) => void>,
  plotlyNew: vi.fn().mockResolvedValue(undefined),
  plotlyPurge: vi.fn(),
  vegaEmbed: vi.fn().mockResolvedValue({ view: { finalize: vi.fn() } }),
}));
vi.mock("./api", () => ({
  api: {
    status: vi.fn().mockResolvedValue({ projects: { items: projects }, live_indexes: { items: [] }, recent_commits: { items: [] }, diagnostics: [], retention_days: 365, truncated: false }),
    projects: vi.fn().mockResolvedValue({ items: projects }),
    overview: vi.fn().mockImplementation(async (_project: string, revision: string) => ({ revision: { requested: revision, state: "ready", commit: revision === "HEAD" ? "abc123" : revision, current_head: "abc123", is_current_head: revision === "abc123" }, repository: { initialized: true, project_home: "/workspace/research", status: { project: "research", branch: "main" }, recent_commits: [] }, current: { active_jobs: 0, recent_runtimes: [] } })),
    commits: vi.fn().mockResolvedValue({ items: [{ id: "older", message: "Older", timestamp: "" }] }),
    dags: vi.fn().mockResolvedValue({ items: [] }), runs: vi.fn().mockResolvedValue({ items: [] }),
    dag: vi.fn().mockResolvedValue({ nodes: [], edges: [] }), customDashboards: vi.fn().mockResolvedValue({ items: [] }), customDashboard: vi.fn(), refreshCustomDashboard: vi.fn(), node: vi.fn().mockResolvedValue({}), commit: vi.fn(), liveIndex: vi.fn(), execution: vi.fn(), logs: vi.fn(), runnableScript: vi.fn(),
    search: vi.fn().mockResolvedValue({ items: [] }), cancelNonce: vi.fn(), cancel: vi.fn(), refs: vi.fn().mockResolvedValue({ revision: { requested: "abc123", state: "ready", commit: "abc123", current_head: "abc123", is_current_head: true }, checkout: { branch: "main", state: "ready" }, current_head: "commit:abc123", selected: { commit: "commit:abc123", labels: ["local:branch:main"] }, branches: [], tags: [], sources: {}, dependencies: { items: [], truncated: false } }),
    registerProject: vi.fn().mockResolvedValue({}), unregisterProject: vi.fn().mockResolvedValue(undefined),
  },
  subscribeToEvents: vi.fn((_scope, onEvent) => { eventHandlers.push(onEvent); return vi.fn(); }), subscribeToLogs: vi.fn(() => vi.fn()),
}));
vi.mock("./components/FlowGraph", () => ({ FlowGraph: () => <div aria-label="Flow graph" /> }));
vi.mock("plotly.js-dist-min", () => ({ default: { newPlot: plotlyNew, purge: plotlyPurge } }));
vi.mock("vega-embed", () => ({ default: vegaEmbed }));

beforeEach(() => {
  history.replaceState(null, "", "/");
  vi.stubGlobal("localStorage", { getItem: vi.fn(), setItem: vi.fn(), removeItem: vi.fn() });
  vi.stubGlobal("matchMedia", vi.fn(() => ({ matches: false, addEventListener: vi.fn(), removeEventListener: vi.fn() })));
  vi.mocked(api.overview).mockClear();
  vi.mocked(api.dags).mockClear();
  vi.mocked(api.status).mockClear();
  vi.mocked(api.projects).mockClear();
  vi.mocked(api.registerProject).mockClear();
  vi.mocked(api.unregisterProject).mockClear();
  vi.mocked(api.dag).mockReset().mockResolvedValue({ id: "dag:detail", nodes: [], edges: [] });
  vi.mocked(api.customDashboards).mockReset().mockResolvedValue({ items: [] });
  vi.mocked(api.customDashboard).mockReset();
  vi.mocked(api.refreshCustomDashboard).mockReset();
  vi.mocked(api.node).mockReset().mockResolvedValue({});
  vi.mocked(api.runnableScript).mockReset();
  eventHandlers.length = 0;
  plotlyNew.mockClear();
  plotlyPurge.mockClear();
  vegaEmbed.mockClear();
});
afterEach(() => { cleanup(); vi.unstubAllGlobals(); });

describe("canonical dashboard routes", () => {
  it("loads Home only at the root", async () => {
    render(<App />);
    expect(await screen.findByRole("heading", { name: "0 commits in the last year" })).toBeVisible();
    expect(location.pathname).toBe("/");
  });

  it("restores a direct concrete DAG route and scopes reads from its URL", async () => {
    history.replaceState(null, "", "/projects/project-1/commits/abc123/dags/dag%3Aone?graphFilter=node");
    render(<App />);
    expect(await screen.findByRole("heading", { name: "DAG Explorer" })).toBeVisible();
    expect(api.dags).toHaveBeenCalledWith({ project: "project-1", revision: "abc123" });
    expect(location.pathname).toBe("/projects/project-1/commits/abc123/dags/dag%3Aone");
  });

  it("does not run a compatible non-eager dashboard until selected", async () => {
    vi.mocked(api.dags).mockResolvedValueOnce({ items: [{ id: "dag:one", name: "One" }] });
    vi.mocked(api.dag).mockResolvedValueOnce({ id: "dag:one", nodes: [], edges: [], tags: ["metrics.v1"] });
    vi.mocked(api.customDashboards).mockResolvedValueOnce({
      items: [{ name: "acme.metrics", tags: ["metrics.v1"], eager: false }],
    });
    vi.mocked(api.customDashboard).mockResolvedValueOnce({
      kind: "vega-lite", spec: { mark: "bar" }, cache_hit: false,
    });
    history.replaceState(null, "", "/projects/project-1/commits/abc123/dags/dag%3Aone");
    render(<App />);

    const selector = await screen.findByRole("combobox", { name: "Select custom dashboard" });
    expect(api.customDashboard).not.toHaveBeenCalled();
    fireEvent.change(selector, { target: { value: "acme.metrics" } });
    await waitFor(() => expect(api.customDashboard).toHaveBeenCalledWith(
      { project: "project-1", revision: "abc123" }, "dag:one", "acme.metrics",
    ));
    expect(location.search).toContain("dashboard=acme.metrics");
    expect(await screen.findByLabelText("Vega-Lite dashboard")).toBeVisible();
    await waitFor(() => expect(vegaEmbed).toHaveBeenCalled());
  });

  it("selects only the first compatible eager dashboard by default", async () => {
    vi.mocked(api.dags).mockResolvedValueOnce({ items: [{ id: "dag:one", name: "One" }] });
    vi.mocked(api.dag).mockResolvedValueOnce({ id: "dag:one", nodes: [], edges: [], tags: ["metrics.v1"] });
    vi.mocked(api.customDashboards).mockResolvedValueOnce({
      items: [
        { name: "acme.first", tags: ["metrics.v1"], eager: true },
        { name: "acme.second", tags: ["metrics.v1"], eager: true },
      ],
      default: "acme.first",
    });
    vi.mocked(api.customDashboard).mockResolvedValueOnce({
      kind: "plotly", data: [{ x: [1] }], layout: {}, config: {}, cache_hit: true,
    });
    history.replaceState(null, "", "/projects/project-1/commits/abc123/dags/dag%3Aone");
    render(<App />);

    await waitFor(() => expect(location.search).toContain("dashboard=acme.first"));
    expect(api.customDashboard).toHaveBeenCalledTimes(1);
    expect(api.customDashboard).toHaveBeenCalledWith(
      { project: "project-1", revision: "abc123" }, "dag:one", "acme.first",
    );
    expect(await screen.findByText("Loaded from local cache")).toBeVisible();
    await waitFor(() => expect(plotlyNew).toHaveBeenCalled());
  });

  it("retains an incompatible dashboard link without executing it", async () => {
    vi.mocked(api.dags).mockResolvedValueOnce({ items: [{ id: "dag:one", name: "One" }] });
    vi.mocked(api.dag).mockResolvedValueOnce({ id: "dag:one", nodes: [], edges: [], tags: ["metrics.v1"] });
    vi.mocked(api.customDashboards).mockResolvedValueOnce({
      items: [{ name: "acme.metrics", tags: ["metrics.v1"], eager: false }],
    });
    history.replaceState(null, "", "/projects/project-1/commits/abc123/dags/dag%3Aone?dashboard=missing");
    render(<App />);

    expect(await screen.findByRole("alert")).toHaveTextContent("not compatible");
    expect(api.customDashboard).not.toHaveBeenCalled();
    expect(location.search).toBe("?dashboard=missing");
  });

  it("refreshes only the selected custom dashboard", async () => {
    vi.mocked(api.dags).mockResolvedValueOnce({ items: [{ id: "dag:one", name: "One" }] });
    vi.mocked(api.dag).mockResolvedValueOnce({ id: "dag:one", nodes: [], edges: [], tags: [] });
    vi.mocked(api.customDashboards).mockResolvedValueOnce({
      items: [{ name: "acme.metrics", tags: [], eager: false }],
    });
    vi.mocked(api.customDashboard).mockResolvedValueOnce({
      kind: "vega-lite", spec: { mark: "point" }, cache_hit: true,
    });
    vi.mocked(api.refreshCustomDashboard).mockResolvedValueOnce({
      kind: "vega-lite", spec: { mark: "bar" }, cache_hit: false,
    });
    history.replaceState(null, "", "/projects/project-1/commits/abc123/dags/dag%3Aone?dashboard=acme.metrics");
    render(<App />);

    fireEvent.click(await screen.findByRole("button", { name: "Refresh custom dashboard" }));
    await waitFor(() => expect(api.refreshCustomDashboard).toHaveBeenCalledWith(
      { project: "project-1", revision: "abc123" }, "dag:one", "acme.metrics",
    ));
    expect(await screen.findByText("Rendered now")).toBeVisible();
  });

  it("bootstraps a selected project through HEAD to its concrete Overview route", async () => {
    render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Select registered project" }));
    fireEvent.click((await screen.findAllByRole("option", { name: /research/ }))[0]);
    await waitFor(() => expect(location.pathname).toBe("/projects/project-1/commits/abc123"));
    expect(api.overview).toHaveBeenCalledWith("project-1", "HEAD");
  });

  it("keeps the most recently selected project when HEAD bootstrap resolves out of order", async () => {
    let resolveFirst: (value: Awaited<ReturnType<typeof api.overview>>) => void;
    let resolveSecond: (value: Awaited<ReturnType<typeof api.overview>>) => void;
    vi.mocked(api.overview)
      .mockImplementationOnce(() => new Promise((resolve) => { resolveFirst = resolve; }))
      .mockImplementationOnce(() => new Promise((resolve) => { resolveSecond = resolve; }));
    render(<App />);

    fireEvent.click(await screen.findByRole("button", { name: "Select registered project" }));
    fireEvent.click((await screen.findAllByRole("option", { name: /workspace\/research/ }))[0]);
    fireEvent.click(screen.getByRole("button", { name: "Select registered project" }));
    fireEvent.click(await screen.findByRole("option", { name: /archive\/research/ }));
    resolveSecond!({ revision: { requested: "HEAD", state: "ready", commit: "second", current_head: "second", is_current_head: true } });
    await waitFor(() => expect(location.pathname).toBe("/projects/project-2/commits/second"));
    resolveFirst!({ revision: { requested: "HEAD", state: "ready", commit: "first", current_head: "first", is_current_head: true } });
    await waitFor(() => expect(location.pathname).toBe("/projects/project-2/commits/second"));
  });

  it("restores an inspector selection from browser history without rewriting its URL", async () => {
    history.replaceState(null, "", "/projects/project-1/commits/abc123");
    render(<App />);
    await screen.findByRole("heading", { name: "Repository snapshot" });

    history.pushState(null, "", "/projects/project-2/commits/second?resource=node%3Aone&resourceType=node&tab=summary");
    window.dispatchEvent(new PopStateEvent("popstate"));

    await waitFor(() => expect(location.pathname).toBe("/projects/project-2/commits/second"));
    expect(location.search).toBe("?resource=node%3Aone&resourceType=node&tab=summary");
    expect(await screen.findByRole("complementary", { name: "node inspector" })).toBeVisible();
  });

  it("gives every node an addressable Value tab without a Runnable tab", async () => {
    vi.mocked(api.node).mockResolvedValueOnce({ id: "node:one", label: "Literal", value_kind: "value", value_type: "int", value: 7 });
    history.replaceState(null, "", "/projects/project-1/commits/abc123?resource=node%3Aone&resourceType=node&tab=value");
    render(<App />);

    const inspector = await screen.findByRole("complementary", { name: "node inspector" });
    expect(within(inspector).getByRole("button", { name: "Value" })).toHaveClass("active");
    expect(within(inspector).queryByRole("button", { name: "Runnable" })).not.toBeInTheDocument();
    expect(await within(inspector).findByText("7")).toBeVisible();
    expect(location.search).toContain("tab=value");
  });

  it("keeps returned and applied runnables in separate Value and Runnable tabs", async () => {
    const runnable = (name: string) => ({ state: "ready", stack: { kind: "script", details: { fn_name: name, script_uri: `s3://bucket/${name}.py` } }, entrypoint: { kind: "script", details: { fn_name: name } }, script: { state: "missing-script-uri", message: `${name} source unavailable` }, prepopulated: [] });
    vi.mocked(api.node).mockResolvedValueOnce({ id: "node:fn", label: "FnNode", value_kind: "runnable", value_type: "Runnable", value_runnable: runnable("returned"), function: { runnable: runnable("applied") } });
    history.replaceState(null, "", "/projects/project-1/commits/abc123?resource=node%3Afn&resourceType=node&tab=runnable");
    render(<App />);

    const inspector = await screen.findByRole("complementary", { name: "node inspector" });
    expect(await within(inspector).findByRole("button", { name: "Runnable" })).toHaveClass("active");
    expect((await within(inspector).findAllByText("applied"))[0]).toBeVisible();
    fireEvent.click(within(inspector).getByRole("button", { name: "Value" }));
    expect(within(inspector).getAllByText("returned")[0]).toBeVisible();
    expect(location.search).toContain("tab=value");
  });

  it("places an FnNode's context-DAG action in the inspector header", async () => {
    vi.mocked(api.node).mockResolvedValueOnce({ id: "node:fn", type: "FnNode", function: { dag: { ref: "dag:context" } } });
    history.replaceState(null, "", "/projects/project-1/commits/abc123/dags/dag%3Aone?resource=node%3Afn&resourceType=node&tab=summary");
    render(<App />);

    const inspector = await screen.findByRole("complementary", { name: "node inspector" });
    expect(await within(inspector).findByText("FnNode", { selector: ".inspector__crumbs span" })).toBeVisible();
    expect(within(inspector).queryByRole("heading", { name: "FnNode" })).not.toBeInTheDocument();
    const action = within(inspector).getByRole("button", { name: "Open context DAG →" });
    expect(action.closest("header")).not.toBeNull();
    fireEvent.click(action);
    await waitFor(() => expect(location.pathname).toBe("/projects/project-1/commits/abc123/dags/dag%3Acontext"));
  });

  it("reads FnNode logs through its persisted function-DAG identity", async () => {
    vi.mocked(api.runs).mockResolvedValueOnce({ items: [{ id: "index:matching", cache_key: "durable-cache" }] });
    vi.mocked(api.node).mockResolvedValueOnce({
      id: "node:fn",
      function: { cache_key: "durable-cache", dag: { ref: "dag:context" } },
    });
    vi.mocked(api.logs).mockResolvedValue({ text: "completed", lines: undefined });
    history.replaceState(null, "", "/projects/project-1/commits/abc123?resource=node%3Afn&resourceType=node&tab=logs");

    render(<App />);

    await waitFor(() => expect(api.logs).toHaveBeenCalledWith(
      { project: "project-1", revision: "abc123" }, "dag:context", "stdout", undefined, "function-dag",
    ));
    expect(await screen.findByText("completed")).toBeVisible();
  });

  it("preserves a restored DAG route while changing browser-history scope", async () => {
    history.replaceState(null, "", "/projects/project-1/commits/abc123/dags/dag%3Aone");
    render(<App />);
    await screen.findByRole("heading", { name: "DAG Explorer" });

    history.pushState(null, "", "/projects/project-2/commits/second/dags/dag%3Atwo");
    window.dispatchEvent(new PopStateEvent("popstate"));

    await waitFor(() => expect(location.pathname).toBe("/projects/project-2/commits/second/dags/dag%3Atwo"));
    expect(await screen.findByLabelText("Flow graph")).toBeVisible();
    expect(api.dag).toHaveBeenCalledWith({ project: "project-2", revision: "second" }, "dag:two");
  });

  it("does not navigate from an obsolete unborn-project HEAD refresh", async () => {
    let resolveHead!: (value: Awaited<ReturnType<typeof api.overview>>) => void;
    vi.mocked(api.overview).mockImplementationOnce(() => new Promise((resolve) => { resolveHead = resolve; }));
    history.replaceState(null, "", "/projects/project-1/unborn");
    render(<App />);
    await waitFor(() => expect(api.overview).toHaveBeenCalledWith("project-1", "HEAD"));

    fireEvent.click(screen.getByRole("link", { name: /DaggerML/ }));
    resolveHead({ revision: { requested: "HEAD", state: "ready", commit: "abc123", current_head: "abc123", is_current_head: true } });

    await screen.findByRole("heading", { name: "0 commits in the last year" });
    await waitFor(() => expect(location.pathname).toBe("/"));
  });

  it("navigates search results by their canonical project and commit route", async () => {
    vi.mocked(api.search).mockResolvedValueOnce({
      items: [{ type: "dag", id: "dag:other", label: "Other DAG", project_id: "project-2", href: "/projects/project-2/commits/other/dags/dag%3Aother" }],
    });
    render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: /Search projects/ }));
    fireEvent.change(screen.getByPlaceholderText("Search projects, refs, commits, and DAGs…"), { target: { value: "other" } });
    fireEvent.click(await screen.findByRole("button", { name: /Other DAG/ }));
    expect(location.pathname).toBe("/projects/project-2/commits/other/dags/dag%3Aother");
  });

  it("clears its resource selection when changing commits", async () => {
    history.replaceState(null, "", "/projects/project-1/commits/abc123?resource=node%3Aone&resourceType=node");
    render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Older older" }));
    expect(location.pathname).toBe("/projects/project-1/commits/older");
    expect(location.search).toBe("");
  });

  it.each(["/status", "/projects", "/history", "/projects/project-1", "/projects/project-1/history", "/projects/project-1/dags"]) (
    "does not recognize or redirect the removed route %s", async (path) => {
      history.replaceState(null, "", path);
      render(<App />);
      expect(await screen.findByRole("heading", { name: "Page not found" })).toBeVisible();
      expect(location.pathname).toBe(path);
    },
  );
});

describe("Home and project navigation", () => {
  it("combines aggregate queues, calendar, diagnostics, and project fields without losing healthy rows", async () => {
    vi.mocked(api.status).mockResolvedValueOnce({ generated_at: "2026-01-01T00:00:00Z", projects: { items: projects }, live_indexes: { items: [] }, recent_commits: { items: [] }, diagnostics: [{ project_id: "project-2", availability: "unavailable", code: "project-read-failed", message: "Project state could not be read", retryable: true }], retention_days: 365, truncated: false });
    render(<App />);
    expect(await screen.findByRole("heading", { name: "Projects" })).toBeVisible();
    expect(screen.getByRole("table", { name: "Registered projects" })).toHaveTextContent("in-sync");
    expect(screen.getByRole("table", { name: "Registered projects" })).toHaveTextContent("2");
    expect(screen.getByText("Project state could not be read")).toBeVisible();
    expect(within(screen.getByRole("row", { name: /project unavailable/ })).getByRole("cell", { name: /project unavailable/ })).toBeDisabled();
  });

  it("opens a Home live-work inspector with project-only current-resource scope", async () => {
    vi.mocked(api.status).mockResolvedValueOnce({ generated_at: "2026-01-01T00:00:00Z", projects: { items: projects }, live_indexes: { items: [{ project_id: "project-1", project_name: "research", index_ref: "index:active", title: "Current work", group: "in-progress", created_at: "2026-01-01T00:00:00Z", dag_ref: "dag:active", links: { dag: "/projects/project-1/commits/abc123/dags/dag%3Aactive" } }] }, recent_commits: { items: [] }, diagnostics: [], retention_days: 365, truncated: false });
    vi.mocked(api.liveIndex).mockResolvedValueOnce({ index_ref: "index:active", title: "Current work", state: "running", group: "in-progress", created_at: "2026-01-01T00:00:00Z", dag: {}, lineage: [], evidence: {}, identifiers: {}, diagnostics: [] });
    render(<App />);

    fireEvent.click(await screen.findByRole("button", { name: /Current work/ }));
    const inspector = await screen.findByRole("complementary", { name: "index inspector" });
    expect(within(inspector).getByRole("link", { name: "Project project-1" })).toHaveAttribute("href", "/projects/project-1/unborn");
    const indexLink = within(inspector).getByRole("link", { name: "Current work" });
    expect(indexLink).toHaveAttribute("href", "/projects/project-1/commits/abc123/dags/dag%3Aactive");
    fireEvent.click(indexLink);
    await waitFor(() => expect(location.pathname).toBe("/projects/project-1/commits/abc123/dags/dag%3Aactive"));
    expect(api.liveIndex).toHaveBeenCalledWith("project-1", "index:active");
    expect(await screen.findByRole("heading", { name: "DAG Explorer" })).toBeVisible();
  });

  it("discloses duplicate project paths to pointer, keyboard, and assistive users", async () => {
    render(<App />);
    const path = await screen.findByText("/workspace/…/research");
    expect(path).toHaveAttribute("tabindex", "0");
    expect(path).toHaveAttribute("data-full", "/workspace/research");
    expect(screen.getByText("Full path: /workspace/research")).toBeInTheDocument();
    path.focus();
    expect(path).toHaveFocus();
  });

  it("adds a project from Home and refreshes project data", async () => {
    render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Add project" }));
    fireEvent.change(screen.getByLabelText("Project path"), { target: { value: "/workspace/new-project" } });
    fireEvent.change(screen.getByLabelText(/Display name/), { target: { value: "New project" } });
    fireEvent.click(within(screen.getByRole("dialog", { name: "Add project" })).getByRole("button", { name: "Add project" }));
    await waitFor(() => expect(api.registerProject).toHaveBeenCalledWith("/workspace/new-project", "New project"));
    await waitFor(() => expect(api.status).toHaveBeenCalledTimes(2));
    expect(api.projects).toHaveBeenCalledTimes(2);
  });

  it("confirms project removal without implying repository deletion", async () => {
    render(<App />);
    fireEvent.click((await screen.findAllByRole("button", { name: "Remove research" }))[0]);
    expect(screen.getByText(/Repository files and history will not be changed/)).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "Remove project" }));
    await waitFor(() => expect(api.unregisterProject).toHaveBeenCalledWith("project-1"));
    await waitFor(() => expect(api.status).toHaveBeenCalledTimes(2));
  });

  it("uses the brand as Home navigation and keeps current-location cues and project destinations available", async () => {
    history.replaceState(null, "", "/projects/project-1/commits/abc123");
    render(<App />);
    expect((await screen.findAllByRole("button", { name: "Overview" }))[0]).toHaveAttribute("aria-current", "page");
    fireEvent.click(screen.getByRole("link", { name: /DaggerML/ }));
    expect(location.pathname).toBe("/");
  });

  it("opens the mobile navigation drawer at a narrow viewport", async () => {
    Object.defineProperty(window, "innerWidth", { configurable: true, value: 480 });
    render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Open navigation" }));
    expect(document.querySelector(".sidebar")).toHaveClass("sidebar--open");
    expect(screen.getAllByRole("button", { name: "Close navigation" }).find((element) => element.classList.contains("sidebar-scrim"))).toBeInTheDocument();
  });
});

describe("revision-scoped project pages", () => {
  it("labels live work as current and highlights a historical selected commit", async () => {
    vi.mocked(api.overview).mockResolvedValueOnce({
      initialized: true, project: "research", branch: "main", active_jobs: 1,
      revision: { requested: "older", state: "ready", commit: "older", current_head: "abc123", is_current_head: false },
      repository: {}, current: {},
    });
    vi.mocked(api.commits).mockResolvedValueOnce({ items: [{ id: "older", message: "Older", timestamp: "", refs: [{ name: "release-1", kind: "tag" }] }, { id: "abc123", message: "Newer", timestamp: "", refs: [{ name: "main", kind: "head" }] }] });
    history.replaceState(null, "", "/projects/project-1/commits/older");
    render(<App />);
    expect(await screen.findByText("Live-index timeboard")).toBeVisible();
    expect(screen.getByRole("heading", { name: "Repository snapshot" })).toBeVisible();
    expect(screen.getByRole("heading", { name: "Current operations" })).toBeVisible();
    expect(screen.getByRole("button", { name: /Older older, selected/ })).toHaveAttribute("aria-current", "true");
    expect(document.querySelector(".ref-badge-group--tag")).toBeTruthy();
    expect(document.querySelector(".ref-badge-group--head")).toBeTruthy();
    expect(screen.getByText("Committed DAGs")).toBeVisible();
    expect(screen.queryByText("Recent DAGs")).not.toBeInTheDocument();
    expect(screen.queryByText("Infrastructure")).not.toBeInTheDocument();
    expect(screen.queryByText(/dml:\/\//)).not.toBeInTheDocument();
  });

  it("separates elapsed live-index timing from actionable active-index rows", async () => {
    vi.mocked(api.status).mockResolvedValueOnce({ generated_at: "2026-01-01T00:00:00Z", projects: { items: projects }, live_indexes: { items: [{ project_id: "project-1", project_name: "research", index_ref: "index:active", title: "Current work", group: "in-progress", created_at: "2026-01-01T00:00:00Z", links: {} }] }, recent_commits: { items: [] }, diagnostics: [], retention_days: 365, truncated: false });
    history.replaceState(null, "", "/projects/project-1/commits/abc123");
    render(<App />);

    const timeboard = (await screen.findByText("Live-index timeboard")).closest("section");
    const indexes = screen.getByText("Active indexes").closest("section");
    expect(timeboard?.querySelector(".live-timeboard")).toBeTruthy();
    expect(indexes?.querySelector(".overview-live-indexes")).toBeTruthy();
    expect(screen.getByLabelText("Elapsed time for live indexes")).toBeVisible();
    expect(screen.getByRole("button", { name: /Current work/ })).toBeVisible();
  });

  it("keeps the selected snapshot stable and relabels operations as current when HEAD advances", async () => {
    vi.mocked(api.overview)
      .mockResolvedValueOnce({ initialized: true, project: "research", revision: { requested: "older", state: "ready", commit: "older", current_head: "older", is_current_head: true }, repository: {}, current: {} })
      .mockResolvedValueOnce({ initialized: true, project: "research", revision: { requested: "older", state: "ready", commit: "older", current_head: "newer", is_current_head: false }, repository: {}, current: {} });
    vi.mocked(api.commits).mockResolvedValueOnce({ items: [{ id: "older", message: "Older", timestamp: "" }] });
    history.replaceState(null, "", "/projects/project-1/commits/older");
    render(<App />);
    await screen.findByRole("button", { name: /Older older, selected/ });
    fireEvent.click(screen.getByRole("button", { name: "Refresh dashboard" }));
    await waitFor(() => expect(screen.getByText("Repository snapshot · historical commit")).toBeVisible());
    expect(location.pathname).toBe("/projects/project-1/commits/older");
    expect(screen.getByText("Live-index timeboard")).toBeVisible();
  });

  it("selects a visible commit with the keyboard and retains focus on the selected mark", async () => {
    vi.mocked(api.commits).mockResolvedValueOnce({ items: [{ id: "older", message: "Older", timestamp: "" }, { id: "abc123", message: "Newer", timestamp: "" }] });
    history.replaceState(null, "", "/projects/project-1/commits/abc123");
    render(<App />);
    const older = await screen.findByRole("button", { name: "Older older" });
    older.focus();
    fireEvent.keyDown(older, { key: "Enter" });
    await waitFor(() => expect(location.pathname).toBe("/projects/project-1/commits/older"));
    expect(screen.getByRole("button", { name: /Older older, selected/ })).toHaveFocus();
  });

  it("marks bounded visible history tips while retaining graph topology selection", async () => {
    vi.mocked(api.commits).mockResolvedValueOnce({ next_cursor: "more", items: [{ id: "older", message: "Older", timestamp: "", parents: ["root"] }, { id: "root", message: "Root", timestamp: "" }] });
    history.replaceState(null, "", "/projects/project-1/commits/abc123");
    render(<App />);
    expect(await screen.findByText(/Visible tips are bounded/)).toBeVisible();
    const older = screen.getByRole("button", { name: "Older older" });
    fireEvent.keyDown(older, { key: "Enter" });
    await waitFor(() => expect(location.pathname).toBe("/projects/project-1/commits/older"));
  });

  it("keeps unmaterialized live ref tips visible but does not select or fetch them", async () => {
    vi.mocked(api.refs).mockResolvedValueOnce({
      revision: { requested: "abc123", state: "ready", commit: "abc123", current_head: "abc123", is_current_head: true }, checkout: { branch: "main", state: "ready" }, current_head: "commit:abc123", selected: { commit: "commit:abc123", labels: [] },
      branches: [{ kind: "branch", name: "main", relation: "in-sync", local: { commit: "commit:abc123", inspectable: true }, live: { commit: "commit:remote", inspectable: false } }], tags: [], sources: {}, dependencies: { items: [], truncated: false },
    });
    history.replaceState(null, "", "/projects/project-1/commits/abc123/refs");
    render(<App />);
    const unavailable = await screen.findByRole("button", { name: /Live remote tip remote, not locally available/ });
    expect(unavailable).toBeDisabled();
    fireEvent.click(unavailable);
    expect(location.pathname).toBe("/projects/project-1/commits/abc123/refs");
  });

  it("groups main-remote and dependency sources and keeps keyboard ref selection on Tags and refs", async () => {
    vi.mocked(api.refs).mockResolvedValueOnce({
      revision: { requested: "abc123", state: "ready", commit: "abc123", current_head: "abc123", is_current_head: true }, checkout: { branch: "main", state: "ready" }, current_head: "commit:abc123", selected: { commit: "commit:abc123", labels: ["local:branch:main"] },
      branches: [{ kind: "branch", name: "ahead", relation: "ahead", local: { commit: "commit:abc123", inspectable: true }, tracking: { commit: "commit:base", inspectable: true }, upstream: "origin/ahead" }, { kind: "branch", name: "unknown", relation: "unknown", live: { commit: "commit:live", inspectable: false } }],
      tags: [{ kind: "tag", name: "release", relation: "conflicting", local: { commit: "commit:abc123", inspectable: true }, live: { commit: "commit:other", inspectable: false } }],
      sources: { live: { diagnostic: { availability: "unauthorized", message: "Live tags require permission" }, branch: { truncated: true } } },
      dependencies: {
        truncated: false,
        items: [{
          name: "models",
          root: "s3://bucket/models",
          branches: [{ kind: "branch", name: "main", relation: "remote-only", fetched: { commit: "commit:dep", inspectable: true } }],
          tags: [{ kind: "tag", name: "release", relation: "conflicting", live: { commit: "commit:remote", inspectable: false } }],
          sources: { live: { diagnostic: { availability: "unavailable", message: "Dependency live refs are unavailable" } } },
        }],
      },
    });
    history.replaceState(null, "", "/projects/project-1/commits/abc123/refs");
    render(<App />);
    expect(await screen.findByText("Main remote")).toBeVisible();
    expect(screen.getByText("Live tags require permission")).toBeVisible();
    expect(screen.getByText("s3://bucket/models")).toBeVisible();
    expect(screen.getByText("Dependency live refs are unavailable")).toBeVisible();
    expect(screen.getAllByText("ahead")[0]).toBeVisible();
    expect(screen.getAllByText("conflicting")[0]).toBeVisible();
    const selectable = screen.getAllByRole("button", { name: /Local tip abc123, select revision/ })[0];
    fireEvent.keyDown(selectable, { key: "Enter" });
    expect(location.pathname).toBe("/projects/project-1/commits/abc123/refs");
  });

  it("removes current partial DAGs and stale route details when HEAD moves", async () => {
    vi.mocked(api.status).mockResolvedValueOnce({ generated_at: "2026-01-01T00:00:00Z", projects: { items: projects }, live_indexes: { items: [{ project_id: "project-1", project_name: "research", index_ref: "index:active", dag_ref: "dag:partial", title: "Current work", group: "in-progress", created_at: "2026-01-01T00:00:00Z", links: {} }] }, recent_commits: { items: [] }, diagnostics: [], retention_days: 365, truncated: false });
    vi.mocked(api.dags)
      .mockResolvedValueOnce({ items: [], live_dags_eligible: true })
      .mockResolvedValueOnce({ items: [], live_dags_eligible: false });
    vi.mocked(api.dag)
      .mockResolvedValueOnce({ id: "dag:partial", nodes: [], edges: [] })
      .mockRejectedValueOnce(new Error("Resource is not available in this revision"));
    history.replaceState(null, "", "/projects/project-1/commits/abc123/dags/dag%3Apartial");
    render(<App />);
    expect(await screen.findByRole("heading", { name: "DAG Explorer" })).toBeVisible();
    expect(screen.queryByText("Committed DAGs and clearly separated current partial DAGs.")).not.toBeInTheDocument();
    eventHandlers.at(-1)?.(new MessageEvent("repository"));
    await waitFor(() => expect(screen.getByRole("heading", { name: "DAG not found in this revision" })).toBeVisible());
  });

  it("keeps live and frozen index outcome colors when their DAG is selected", async () => {
    vi.mocked(api.status).mockResolvedValueOnce({ generated_at: "2026-01-01T00:00:00Z", projects: { items: projects }, live_indexes: { items: [
      { project_id: "project-1", project_name: "research", index_ref: "index:active", dag_ref: "dag:active", title: "Active index", group: "in-progress", created_at: "2026-01-01T00:00:00Z", links: {} },
      { project_id: "project-1", project_name: "research", index_ref: "frozenindex:frozen", dag_ref: "dag:frozen", title: "Frozen index", group: "needs-attention", created_at: "2026-01-01T00:00:00Z", links: {} },
    ] }, recent_commits: { items: [] }, diagnostics: [], retention_days: 365, truncated: false });
    vi.mocked(api.dags).mockResolvedValueOnce({ items: [{ id: "dag:normal", name: "Normal DAG", status: "ready" }, { id: "dag:error", name: "Error DAG", status: "error" }], live_dags_eligible: true });
    vi.mocked(api.dag).mockResolvedValueOnce({ id: "dag:frozen", status: "ready", nodes: [], edges: [] });
    history.replaceState(null, "", "/projects/project-1/commits/abc123/dags/dag%3Afrozen");

    render(<App />);

    await waitFor(() => expect(api.dag).toHaveBeenCalledWith({ project: "project-1", revision: "abc123" }, "dag:frozen"));
    const picker = within(screen.getByLabelText("DAGs"));
    expect(picker.getByText("Active index", { selector: "button strong" }).closest("button")?.querySelector(".dag-picker__icon")).toHaveClass("dag-picker__icon--index");
    expect(picker.getByText("Frozen index", { selector: "button strong" }).closest("button")?.querySelector(".dag-picker__icon")).toHaveClass("dag-picker__icon--attention");
    expect(picker.getByText("Normal DAG", { selector: "button strong" }).closest("button")?.querySelector(".dag-picker__icon")).toHaveClass("dag-picker__icon--normal");
    expect(picker.getByText("Error DAG", { selector: "button strong" }).closest("button")?.querySelector(".dag-picker__icon")).toHaveClass("dag-picker__icon--failure");
  });

  it("loads a revision-reachable function DAG that is absent from the top-level inventory", async () => {
    vi.mocked(api.dags).mockResolvedValueOnce({ items: [], live_dags_eligible: false });
    vi.mocked(api.dag).mockResolvedValueOnce({ id: "dag:function", function: {}, nodes: [], edges: [] });
    history.replaceState(null, "", "/projects/project-1/commits/older/dags/dag%3Afunction");

    render(<App />);

    expect(await screen.findByText("Function context DAG")).toBeVisible();
    expect(api.dag).toHaveBeenCalledWith({ project: "project-1", revision: "older" }, "dag:function");
    expect(screen.queryByRole("heading", { name: "DAG not found in this revision" })).not.toBeInTheDocument();
  });
});
