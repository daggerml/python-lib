// @vitest-environment jsdom
import { afterEach, describe, expect, it, vi } from "vitest";
import { api, subscribeToEvents } from "./api";

const scope = { project: "project 1", revision: "abc123" };
const response = (value: unknown) => new Response(JSON.stringify(value), { headers: { "Content-Type": "application/json" } });
afterEach(() => { vi.unstubAllGlobals(); });

describe("revision-scoped dashboard API helpers", () => {
  it("sends project and concrete revision for workspace reads without local storage", async () => {
    const fetch = vi.fn().mockResolvedValue(response({ items: [] }));
    vi.stubGlobal("fetch", fetch);
    vi.stubGlobal("localStorage", { getItem: () => "stale-project" });
    await api.dags(scope);
    expect(fetch.mock.calls[0][0]).toBe("/api/v1/dags?project=project+1&revision=abc123");
  });

  it("resolves HEAD only when bootstrap explicitly requests it", async () => {
    const fetch = vi.fn().mockResolvedValue(response({ revision: { requested: "HEAD", state: "ready", commit: "abc123", is_current_head: true } }));
    vi.stubGlobal("fetch", fetch);
    await api.overview("project-1", "HEAD");
    expect(fetch.mock.calls[0][0]).toBe("/api/v1/overview?project=project-1&revision=HEAD");
  });

  it("uses current checkout status instead of treating a populated revision as unborn", async () => {
    const fetch = vi.fn().mockResolvedValue(response({
      revision: { requested: "abc123", state: "ready", commit: "abc123", is_current_head: true },
      repository: { commit: { id: "abc123" } },
      current: { status: { branch: "main", commit: "abc123" }, config: { remote: {} } },
    }));
    vi.stubGlobal("fetch", fetch);

    const overview = await api.overview("project-1", "abc123");

    expect(overview.branch).toBe("main");
    expect(overview.head).toBe("abc123");
  });

  it("reads a live index with its project-only current-resource scope", async () => {
    const fetch = vi.fn().mockResolvedValue(response({}));
    vi.stubGlobal("fetch", fetch);
    await api.liveIndex("project 1", "index:active");
    expect(fetch.mock.calls[0][0]).toBe("/api/v1/live-indexes/index%3Aactive?project=project%201");
  });

  it("scopes search to the selected project and revision", async () => {
    const fetch = vi.fn().mockResolvedValue(response({ items: [] }));
    vi.stubGlobal("fetch", fetch);
    await api.search("model", scope);
    expect(fetch.mock.calls[0][0]).toBe("/api/v1/search?q=model&project=project%201&revision=abc123");
  });

  it("unwraps commit details returned with revision metadata", async () => {
    const fetch = vi.fn().mockResolvedValue(response({
      revision: { requested: "abc123", state: "ready", commit: "abc123" },
      repository: { commit: { id: "abc123", message: "Record experiment" } },
    }));
    vi.stubGlobal("fetch", fetch);

    const commit = await api.commit(scope, "abc123");

    expect(commit).toMatchObject({ id: "abc123", message: "Record experiment", short_id: "abc123" });
    expect(fetch.mock.calls[0][0]).toBe("/api/v1/commits/abc123?project=project+1&revision=abc123");
  });

  it("preserves structured commit ref labels for display", async () => {
    const fetch = vi.fn().mockResolvedValue(response({
      items: [{ id: "abc123", refs: [{ kind: "head", name: "main" }, { kind: "tag", name: "v1.0" }] }],
    }));
    vi.stubGlobal("fetch", fetch);

    const commits = await api.commits(scope);

    expect(commits.items[0].refs).toEqual([{ kind: "head", name: "main" }, { kind: "tag", name: "v1.0" }]);
  });

  it("uses the normalized scoped search fields", async () => {
    const fetch = vi.fn().mockResolvedValue(response({
      items: [{ type: "dag", id: "dag:experiment", label: "Experiment", href: "/projects/project-1/commits/abc123" }],
    }));
    vi.stubGlobal("fetch", fetch);

    const result = await api.search("experiment", scope);

    expect(result.items?.[0]).toMatchObject({ type: "dag", id: "dag:experiment", label: "Experiment" });
  });

  it("scopes event subscriptions explicitly", () => {
    const close = vi.fn();
    vi.stubGlobal("EventSource", vi.fn(function EventSourceMock() {
      return { addEventListener: vi.fn(), close };
    }));
    const stop = subscribeToEvents(scope, vi.fn());
    expect(EventSource).toHaveBeenCalledWith("/api/v1/events?project=project+1&revision=abc123");
    stop();
    expect(close).toHaveBeenCalled();
  });
});
