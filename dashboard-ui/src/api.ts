import type { Commit, CustomDashboardInventory, CustomDashboardResult, Dag, DagInventory, DashboardProject, Execution, Fndag, GraphNode, LiveIndexDetail, Overview, Paginated, ProjectScope, RefLabel, RefsEnvelope, Remote, Runnable, ScriptEvidence, StatusPayload } from "./types";

const API_ROOT = "/api/v1";
const TOKEN_KEY = "daggerml-dashboard-token";

function authToken(): string | null {
  const fragment = new URLSearchParams(window.location.hash.replace(/^#/, ""));
  const fragmentToken = fragment.get("token");
  if (fragmentToken) {
    sessionStorage.setItem(TOKEN_KEY, fragmentToken);
    history.replaceState(null, "", `${window.location.pathname}${window.location.search}`);
    return fragmentToken;
  }
  return sessionStorage.getItem(TOKEN_KEY);
}

export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
    readonly code?: string,
  ) {
    super(message);
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const token = authToken();
  const headers = new Headers(init?.headers);
  headers.set("Accept", "application/json");
  if (init?.body) headers.set("Content-Type", "application/json");
  if (token) headers.set("Authorization", `Bearer ${token}`);
  const response = await fetch(`${API_ROOT}${path}`, { ...init, headers });
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new ApiError(body.error?.message ?? body.detail ?? body.message ?? `Request failed (${response.status})`, response.status, body.error?.code);
  }
  if (response.status === 204) return undefined as T;
  return response.json() as Promise<T>;
}

function scoped(path: string, scope: ProjectScope): string {
  const params = new URLSearchParams(path.includes("?") ? path.slice(path.indexOf("?") + 1) : "");
  params.set("project", scope.project);
  params.set("revision", scope.revision);
  return `${path.split("?", 1)[0]}?${params}`;
}

const asPage = <T>(value: Paginated<T> | T[]): Paginated<T> =>
  Array.isArray(value) ? { items: value } : { ...value, items: value.items ?? [] };

export const api = {
  projects: () => request<{ items: DashboardProject[]; default_project_id?: string }>("/projects"),
  status: async (cursors?: { project_cursor?: string; live_cursor?: string; commit_cursor?: string; limit?: number }) => {
    const readPage = (pageCursors?: { project_cursor?: string; live_cursor?: string; commit_cursor?: string; limit?: number }) => {
      const params = new URLSearchParams();
      if (pageCursors?.project_cursor) params.set("project_cursor", pageCursors.project_cursor);
      if (pageCursors?.live_cursor) params.set("live_cursor", pageCursors.live_cursor);
      if (pageCursors?.commit_cursor) params.set("commit_cursor", pageCursors.commit_cursor);
      if (pageCursors?.limit) params.set("limit", String(pageCursors.limit));
      return request<StatusPayload>(`/status${params.size ? `?${params}` : ""}`);
    };
    if (cursors?.project_cursor || cursors?.live_cursor || cursors?.commit_cursor) return readPage(cursors);

    const result = await readPage(cursors);
    let next = {
      project_cursor: result.projects.next_cursor ?? undefined,
      live_cursor: result.live_indexes.next_cursor ?? undefined,
      commit_cursor: result.recent_commits.next_cursor ?? undefined,
    };
    for (let pageNumber = 1; pageNumber < 100 && (next.project_cursor || next.live_cursor || next.commit_cursor); pageNumber += 1) {
      const page = await readPage({ ...next, limit: cursors?.limit });
      result.projects.items.push(...page.projects.items);
      result.live_indexes.items.push(...page.live_indexes.items);
      result.recent_commits.items.push(...page.recent_commits.items);
      result.truncated ||= page.truncated;
      next = {
        project_cursor: page.projects.next_cursor ?? undefined,
        live_cursor: page.live_indexes.next_cursor ?? undefined,
        commit_cursor: page.recent_commits.next_cursor ?? undefined,
      };
    }
    result.projects.next_cursor = next.project_cursor;
    result.live_indexes.next_cursor = next.live_cursor;
    result.recent_commits.next_cursor = next.commit_cursor;
    if (next.project_cursor || next.live_cursor || next.commit_cursor) result.truncated = true;
    return result;
  },
  registerProject: (path: string, name?: string) => request<DashboardProject>("/projects", { method: "POST", body: JSON.stringify({ path, name }) }),
  unregisterProject: (id: string) => request<void>(`/projects/${encodeURIComponent(id)}`, { method: "DELETE" }),
  overview: async (project: string, revision: string) => normalizeOverview(await request<Record<string, unknown>>(scoped("/overview", { project, revision }))),
  commits: async (scope: ProjectScope, cursor?: string) => {
    const page = asPage(await request<Paginated<Record<string, unknown>> | Record<string, unknown>[]>(
      scoped(`/commits${cursor ? `?cursor=${encodeURIComponent(cursor)}` : ""}`, scope),
    ));
    return { ...page, items: page.items.map(normalizeCommit) };
  },
  commit: async (scope: ProjectScope, id: string) => {
    const response = await request<Record<string, unknown>>(scoped(`/commits/${encodeURIComponent(id)}`, scope));
    return normalizeCommit(record(record(response.repository).commit));
  },
  dags: async (scope: ProjectScope) => {
    const page = asPage(await request<Paginated<Record<string, unknown>> | Record<string, unknown>[]>(scoped("/dags", scope)));
    return { ...page, items: page.items.map(normalizeDag) } as DagInventory;
  },
  dag: async (scope: ProjectScope, id: string): Promise<Dag> => hydrateFunction(scope, normalizeDag(await request<Record<string, unknown>>(scoped(`/dags/${encodeURIComponent(id)}`, scope)))),
  customDashboards: (scope: ProjectScope, id: string) =>
    request<CustomDashboardInventory>(scoped(`/dags/${encodeURIComponent(id)}/dashboards`, scope)),
  customDashboard: (scope: ProjectScope, id: string, name: string) =>
    request<CustomDashboardResult>(scoped(`/dags/${encodeURIComponent(id)}/dashboard?name=${encodeURIComponent(name)}`, scope)),
  refreshCustomDashboard: (scope: ProjectScope, id: string, name: string) =>
    request<CustomDashboardResult>(scoped(`/dags/${encodeURIComponent(id)}/dashboard/refresh`, scope), {
      method: "POST",
      body: JSON.stringify({ name }),
    }),
  node: async (scope: ProjectScope, id: string): Promise<Record<string, unknown>> => {
    const raw = await request<Record<string, unknown>>(scoped(`/nodes/${encodeURIComponent(id)}`, scope));
    const description = record(raw.description);
    return hydrateFunction(scope, {
      ...raw,
      ...description,
      id: String(description.id ?? id),
      label: String(description.name ?? description.type ?? id.slice(0, 14)),
    });
  },
  liveIndex: (project: string, id: string) => request<LiveIndexDetail>(`/live-indexes/${encodeURIComponent(id)}?project=${encodeURIComponent(project)}`),
  runs: async (scope: ProjectScope) => {
    const page = asPage(await request<Paginated<Record<string, unknown>> | Record<string, unknown>[]>(scoped("/executions", scope)));
    const graph = await request<Record<string, unknown>>(scoped("/executions/graph", scope)).catch((): Record<string, unknown> => ({}));
    const graphNodes = record(graph.nodes);
    const parents = new Map<string, string>();
    for (const [parentId, raw] of Object.entries(graphNodes)) {
      for (const childId of strings(record(raw).children)) parents.set(childId, parentId);
    }
    const items = Object.entries(graphNodes).map(([id, raw]) => normalizeExecution({
      id,
      execution: { ...record(raw), parent_id: parents.get(id) },
    }));
    return { ...page, items: items.length ? items : page.items.map(normalizeExecution) };
  },
  execution: async (scope: ProjectScope, id: string) => {
    const result = normalizeExecutionDetail(id, await request<Record<string, unknown>>(scoped(`/executions/${encodeURIComponent(id)}`, scope)));
    const scriptIndex = result.runnable_chain?.findIndex((item) => item.type === "script") ?? -1;
    if (scriptIndex >= 0) {
      try {
        const script = await request<{ uri?: string; source?: string; truncated?: boolean }>(scoped(`/executions/${encodeURIComponent(id)}/script`, scope));
        result.runnable_chain![scriptIndex] = { ...result.runnable_chain![scriptIndex], ...script };
      } catch {
        // Resource details remain useful when script content is unavailable.
      }
    }
    try {
        return { ...result, fndag: await request<Fndag>(scoped(`/fndags/${encodeURIComponent(id)}`, scope)) };
    } catch {
      return result;
    }
  },
  logs: async (scope: ProjectScope, id: string, stream: "stdout" | "stderr" = "stdout", cursor?: string, source: "execution" | "function-dag" = "execution") => {
    const root = source === "function-dag" ? "/function-dags" : "/executions";
    const result = await request<{ text?: string; events?: Array<{ timestamp?: number; message?: string }>; next_cursor?: string; has_more?: boolean }>(
      scoped(`${root}/${encodeURIComponent(id)}/logs/${stream}${cursor ? `?cursor=${encodeURIComponent(cursor)}` : ""}`, scope),
    );
    return {
      ...result,
      lines: result.events?.map((event) => `${event.timestamp ? new Date(event.timestamp).toISOString() : ""} ${event.message ?? ""}`.trim()),
    };
  },
  runnableScript: async (scope: ProjectScope, href: string): Promise<ScriptEvidence> => {
    try {
      return { state: "available", ...await request<Omit<ScriptEvidence, "state">>(scoped(href.replace(/^\/api\/v1/, ""), scope)) };
    } catch (reason) {
      if (reason instanceof ApiError) return { state: "unavailable", code: reason.code, message: reason.message };
      return { state: "unavailable", message: reason instanceof Error ? reason.message : String(reason) };
    }
  },
  refs: (scope: ProjectScope) => request<RefsEnvelope>(scoped("/refs", scope)),
  remotes: async (scope: ProjectScope) => ({ items: normalizeRemotes(await request<Record<string, unknown>>(scoped("/remotes", scope))) }),
  search: (query: string, scope?: ProjectScope) =>
    request<{ items?: Array<{ type: string; id: string; label?: string; detail?: string; project_id?: string; href?: string }> }>(
      `/search?q=${encodeURIComponent(query)}${scope ? `&project=${encodeURIComponent(scope.project)}&revision=${encodeURIComponent(scope.revision)}` : ""}`,
    ),
  cancelNonce: (scope: ProjectScope, id: string) =>
    request<{ nonce: string }>(scoped(`/executions/${encodeURIComponent(id)}/cancel-confirmation`, scope), { method: "POST" }),
  cancel: (scope: ProjectScope, id: string, nonce: string) =>
    request<{ accepted: boolean; execution_id: string; summary: Record<string, unknown> }>(scoped(`/executions/${encodeURIComponent(id)}/cancel`, scope), {
      method: "POST",
      body: JSON.stringify({ mode: "full", nonce }),
    }),
};

async function hydrateFunction<T extends object>(scope: ProjectScope, raw: T): Promise<T> {
  void scope;
  const object = raw as Record<string, unknown>;
  const context = record(object.function);
  if (!Object.keys(context).length) return raw;
  return { ...raw, function: context } as T;
}

function record(value: unknown): Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

function strings(value: unknown): string[] {
  return Array.isArray(value) ? value.map(String) : [];
}

function refLabels(value: unknown): Array<RefLabel | string> {
  if (!Array.isArray(value)) return [];
  return value.reduce<Array<RefLabel | string>>((labels, entry) => {
    if (typeof entry === "string") {
      labels.push(entry);
      return labels;
    }
    const label = record(entry);
    if (typeof label.name !== "string") return labels;
    const kind = label.kind;
    labels.push({ name: label.name, kind: kind === "head" || kind === "branch" || kind === "tag" || kind === "remote" ? kind : undefined });
    return labels;
  }, []);
}

function normalizeCommit(raw: Record<string, unknown>): Commit {
  const id = String(raw.id ?? raw.commit ?? "unknown");
  const dags = record(raw.dags);
  return {
    ...raw,
    id,
    short_id: id.slice(0, 8),
    message: typeof raw.message === "string" ? raw.message : undefined,
    author: typeof raw.author === "string" ? raw.author : undefined,
    timestamp: isoTime(raw.timestamp ?? raw.created),
    parents: strings(raw.parents).map((parent) => parent.replace(/^commit:/, "")),
    refs: refLabels(raw.refs),
    dag_count: Object.keys(dags).length,
  };
}

function normalizeDag(raw: Record<string, unknown>): Dag {
  const nodeValues = Array.isArray(raw.nodes) ? raw.nodes : undefined;
  const argvId = typeof raw.argv === "string" ? raw.argv : undefined;
  const resultId = typeof raw.result === "string" ? raw.result : undefined;
  const nodes = nodeValues?.map((value) => {
    const node = record(value);
    const id = String(node.id ?? node.ref ?? node.node ?? "unknown");
    const kind = String(node.kind ?? node.type ?? "node").replace(/Node$/, "").toLowerCase();
    const role = kind === "error" ? "error" : id === resultId ? "result" : id === argvId ? "argv" : "intermediate";
    return {
      ...node,
      id,
      label: String(node.label ?? node.name ?? node.function ?? id.slice(0, 14)),
      kind,
      role,
      status: node.is_error ? "error" : String(node.status ?? "ready"),
      function: typeof node.function === "string" ? node.function : undefined,
      inputs: strings(node.argv),
    } as GraphNode;
  });
  const hasError = Object.prototype.hasOwnProperty.call(raw, "error");
  return {
    ...raw,
    id: String(raw.id ?? "unknown"),
    name: typeof raw.name === "string" ? raw.name : undefined,
    node_count: typeof raw.node_count === "number" ? raw.node_count : nodes?.length,
    status: typeof raw.status === "string" ? raw.status : hasError ? raw.error ? "error" : "ready" : undefined,
    commit_id: String(raw.commit_id ?? raw.commit ?? ""),
    nodes,
    edges: Array.isArray(raw.edges) ? raw.edges.map((value, index) => {
      const edge = record(value);
      return { id: String(edge.id ?? index), source: String(edge.source), target: String(edge.target), label: typeof edge.kind === "string" ? edge.kind : undefined };
    }) : [],
  };
}

function normalizeExecution(raw: Record<string, unknown>): Execution {
  const execution = record(raw.execution);
  const id = String(execution.execution_id ?? execution.id ?? raw.id ?? "unknown");
  const lifecycle = String(execution.lifecycle ?? raw.status ?? (raw.execution_diagnostic ? "unavailable" : "pending"));
  return {
    ...raw,
    ...execution,
    id,
    name: String(raw.message ?? raw.name ?? raw.dag ?? `Runtime ${id.slice(0, 8)}`),
    status: lifecycle === "canceled" ? "cancelled" : lifecycle,
    executor: typeof execution.executor === "string" ? execution.executor : undefined,
    progress: typeof execution.progress === "number" ? execution.progress : undefined,
    started_at: isoTime(execution.started_at ?? execution.created ?? raw.created),
    updated_at: isoTime(execution.updated_at ?? execution.modified),
    parent_id: typeof execution.parent_id === "string" ? execution.parent_id : undefined,
    children: strings(execution.children ?? execution.downstream),
    cache_key: typeof execution.cache_key === "string" ? execution.cache_key : undefined,
  };
}

function isoTime(value: unknown): string {
  if (typeof value === "number") return new Date(value < 10_000_000_000 ? value * 1000 : value).toISOString();
  if (typeof value === "string" && /^\d+(?:\.\d+)?$/.test(value)) {
    const number = Number(value);
    return new Date(number < 10_000_000_000 ? number * 1000 : number).toISOString();
  }
  return typeof value === "string" ? value : "";
}

function normalizeExecutionDetail(id: string, raw: Record<string, unknown>): Execution {
  const result = normalizeExecution({ id, execution: raw.record });
  return {
    ...raw,
    ...result,
    runnable_chain: flattenResources(raw.runnable),
  };
}

function flattenResources(value: unknown): Runnable[] {
  const chain: Runnable[] = [];
  let current: unknown = value;
  while (Object.keys(record(current)).length) {
    const item = record(current);
    const details = record(item.details);
    chain.push({ type: String(item.kind ?? item.type ?? "resource"), ...details });
    current = item.sub;
  }
  return chain;
}

function normalizeOverview(raw: Record<string, unknown>): Overview {
  const repository = record(raw.repository);
  const current = record(raw.current);
  // Checkout and configuration are current operational facts.  Repository is
  // deliberately scoped to the selected immutable revision, so it does not
  // carry either value in the workspace overview response.
  const status = record(current.status);
  const config = record(current.config);
  const remote = record(config.remote);
  const revision = record(raw.revision);
  const runtimes = Array.isArray(current.recent_runtimes) ? current.recent_runtimes.map((item) => normalizeExecution(record(item))) : [];
  return {
    initialized: repository.initialized !== false,
    project: String(status.project ?? status.name ?? String(repository.project_home ?? "").split("/").filter(Boolean).at(-1) ?? "Local project"),
    project_home: typeof repository.project_home === "string" ? repository.project_home : undefined,
    branch: String(status.branch ?? status.ref ?? (status.detached ? "Detached HEAD" : "Unborn HEAD")),
    head: typeof status.commit === "string" ? status.commit : String(record(status.commit).id ?? ""),
    ahead: Number(status.ahead ?? 0),
    behind: Number(status.behind ?? 0),
    active_jobs: Number(current.active_jobs ?? raw.active_jobs ?? 0),
    open_runtimes: runtimes.length,
    remote_status: remote.root ? "healthy" : "local",
    executor_status: Number(current.active_jobs ?? raw.active_jobs ?? 0) ? "Active" : "Ready",
    recent_commits: Array.isArray(repository.recent_commits) ? repository.recent_commits.map((item) => normalizeCommit(record(item))) : [],
    recent_dags: [],
    message: typeof raw.diagnostic === "string" ? raw.diagnostic : undefined,
    revision: typeof revision.requested === "string" && (revision.state === "ready" || revision.state === "unborn") ? {
      requested: revision.requested,
      state: revision.state,
      ...(typeof revision.commit === "string" ? { commit: revision.commit } : {}),
      ...(typeof revision.current_head === "string" ? { current_head: revision.current_head } : {}),
      is_current_head: revision.is_current_head === true,
    } : undefined,
    repository,
    current,
  };
}

function normalizeRemotes(raw: Record<string, unknown>): Remote[] {
  const tracking = Array.isArray(raw.tracking) ? raw.tracking.map(record) : [];
  const live = record(raw.live);
  const liveRefs = Array.isArray(live.refs) ? live.refs.map(record) : [];
  const configured = record(raw.configured);
  if (!configured.root && !tracking.length && !liveRefs.length) return [];
  return [{
    name: "origin",
    url: typeof configured.root === "string" ? configured.root : undefined,
    status: live.diagnostic ? "error" : "healthy",
    branches: liveRefs.filter((item) => item.kind === "branch").length || tracking.filter((item) => item.kind === "branch").length,
    tags: liveRefs.filter((item) => item.kind === "tag").length || tracking.filter((item) => item.kind === "tag").length,
  }];
}

export function subscribeToEvents(scope: ProjectScope, onEvent: (event: MessageEvent) => void): () => void {
  const token = authToken();
  const params = new URLSearchParams();
  if (token) params.set("token", token);
  params.set("project", scope.project);
  params.set("revision", scope.revision);
  const url = `${API_ROOT}/events${params.size ? `?${params}` : ""}`;
  const events = new EventSource(url);
  ["update", "repository", "execution", "cancellation", "log"].forEach((name) => events.addEventListener(name, onEvent));
  return () => events.close();
}

export function subscribeToLogs(
  scope: ProjectScope,
  resourceId: string,
  stream: "stdout" | "stderr",
  onEvent: (event: MessageEvent) => void,
  source: "execution" | "function-dag" = "execution",
): () => void {
  const token = authToken();
  const params = new URLSearchParams();
  if (token) params.set("token", token);
  params.set("project", scope.project);
  params.set("revision", scope.revision);
  const suffix = params.size ? `?${params}` : "";
  const root = source === "function-dag" ? "/function-dags" : "/executions";
  const events = new EventSource(`${API_ROOT}${root}/${encodeURIComponent(resourceId)}/logs/${stream}/events${suffix}`);
  events.addEventListener("log", onEvent);
  return () => events.close();
}
