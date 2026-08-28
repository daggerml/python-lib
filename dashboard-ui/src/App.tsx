import {
  Activity,
  AlertTriangle,
  Archive,
  Box,
  Check,
  ChevronDown,
  CircleStop,
  Clock3,
  Command,
  Copy,
  GitBranch,
  GitCommitHorizontal,
  FolderKanban,
  LayoutDashboard,
  ListTodo,
  Maximize2,
  Menu,
  Minimize2,
  Moon,
  Network,
  PanelLeftClose,
  PanelLeftOpen,
  PanelRightClose,
  Plus,
  RefreshCw,
  Search,
  Server,
  Sun,
  TerminalSquare,
  Trash2,
  X,
  Zap,
} from "lucide-react";
import { useCallback, useEffect, useMemo, useRef, useState, type CSSProperties, type FormEvent, type KeyboardEvent as ReactKeyboardEvent, type MouseEvent as ReactMouseEvent, type PointerEvent as ReactPointerEvent, type ReactNode } from "react";
import dagMark from "./assets/daggerml-dag-mark.png";
import { api, subscribeToEvents, subscribeToLogs } from "./api";
import { FlowGraph } from "./components/FlowGraph";
import { CommitGraph } from "./components/CommitGraph";
import { CustomDashboardPanel } from "./components/CustomDashboardPanel";
import { StatusPill } from "./components/StatusPill";
import type {
  Commit,
  Dag,
  DagInventory,
  DashboardProject,
  Execution,
  GraphEdge,
  GraphNode,
  Overview,
  Paginated,
  PageId,
  ProjectScope,
  RefGroup,
  RefsEnvelope,
  RunnableInspection,
  Selection,
  StatusCommit,
  StatusLiveIndex,
  StatusPayload,
} from "./types";

const PROJECT_NAV: Array<{ id: PageId; label: string; icon: typeof LayoutDashboard; shortcut: string }> = [
  { id: "overview", label: "Overview", icon: LayoutDashboard, shortcut: "G O" },
  { id: "dags", label: "DAG Explorer", icon: Network, shortcut: "G D" },
  { id: "refs", label: "Tags and refs", icon: GitBranch, shortcut: "G R" },
];
const NAV = PROJECT_NAV;

interface BrowserRoute {
  page: PageId;
  projectId?: string;
  commitId?: string;
  dagId?: string;
  graphFilter?: string;
  dashboard?: string;
  selection?: Selection;
  inspectorTab?: string;
  invalid?: boolean;
}

function readBrowserRoute(): BrowserRoute {
  let parts: string[];
  try { parts = window.location.pathname.split("/").filter(Boolean).map(decodeURIComponent); } catch { return { page: "home", invalid: true }; }
  let page: PageId = "home";
  let projectId: string | undefined;
  let commitId: string | undefined;
  let dagId: string | undefined;
  let invalid = false;
  if (parts.length) {
    if (parts[0] !== "projects" || !parts[1]) invalid = true;
    else if (parts.length === 3 && parts[2] === "unborn") { projectId = parts[1]; page = "unborn"; }
    else if (parts[2] === "commits" && parts[3]) {
      projectId = parts[1]; commitId = parts[3];
      if (parts.length === 4) page = "overview";
      else if (parts[4] === "dags" && parts.length === 5) page = "dags";
      else if (parts[4] === "dags" && parts[5] && parts.length === 6) { page = "dags"; dagId = parts[5]; }
      else if (parts[4] === "refs" && parts.length === 5) page = "refs";
      else invalid = true;
    } else invalid = true;
  }
  const params = new URLSearchParams(window.location.search);
  const resource = params.get("resource") ?? undefined;
  const resourceType = params.get("resourceType") as Selection["type"] | null;
  const requestedTab = params.get("tab") ?? undefined;
  const inspectorTab = requestedTab;
  return {
    page,
    projectId,
    commitId,
    dagId,
    graphFilter: params.get("graphFilter") ?? undefined,
    dashboard: params.get("dashboard") ?? undefined,
    selection: resource && resourceType ? { type: resourceType, id: resource, project_id: projectId } : undefined,
    inspectorTab,
    invalid,
  };
}

function routePath(page: PageId, projectId?: string, commitId?: string, dagId?: string): string {
  if (page === "home") return "/";
  if (!projectId) return "/";
  const root = `/projects/${encodeURIComponent(projectId)}`;
  if (page === "unborn") return `${root}/unborn`;
  if (!commitId) return root;
  const commitRoot = `${root}/commits/${encodeURIComponent(commitId)}`;
  if (page === "dags") return dagId ? `${commitRoot}/dags/${encodeURIComponent(dagId)}` : `${commitRoot}/dags`;
  if (page === "refs") return `${commitRoot}/refs`;
  return commitRoot;
}

function useLoad<T>(loader: () => Promise<T>, dependencies: unknown[] = []) {
  const [data, setData] = useState<T>();
  const [error, setError] = useState<string>();
  const [loading, setLoading] = useState(true);
  const generation = useRef(0);
  const load = useCallback(() => {
    const currentGeneration = ++generation.current;
    setLoading(true);
    loader()
      .then((value) => {
        if (generation.current !== currentGeneration) return;
        setData(value);
        setError(undefined);
      })
      .catch((reason: unknown) => {
        if (generation.current === currentGeneration) {
          setError(reason instanceof Error ? reason.message : String(reason));
        }
      })
      .finally(() => {
        if (generation.current === currentGeneration) setLoading(false);
      });
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, dependencies);
  useEffect(load, [load]);
  return { data, error, loading, reload: load };
}

function ProjectSwitcher({
  projects,
  selectedProject,
  fallbackPath,
  onSelect,
}: {
  projects: DashboardProject[];
  selectedProject?: DashboardProject;
  fallbackPath?: string;
  onSelect: (id: string) => void;
}) {
  const [open, setOpen] = useState(false);
  const rootRef = useRef<HTMLDivElement>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const menuRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    const selectedOption = menuRef.current?.querySelector<HTMLElement>("[role=option][aria-selected=true]");
    selectedOption?.focus();
    const closeOnOutsideClick = (event: PointerEvent) => {
      if (!rootRef.current?.contains(event.target as Node)) setOpen(false);
    };
    const closeOnEscape = (event: globalThis.KeyboardEvent) => {
      if (event.key !== "Escape") return;
      setOpen(false);
      triggerRef.current?.focus();
    };
    document.addEventListener("pointerdown", closeOnOutsideClick);
    document.addEventListener("keydown", closeOnEscape);
    return () => {
      document.removeEventListener("pointerdown", closeOnOutsideClick);
      document.removeEventListener("keydown", closeOnEscape);
    };
  }, [open]);

  const moveOptionFocus = (event: ReactKeyboardEvent<HTMLDivElement>) => {
    if (!["ArrowDown", "ArrowUp", "Home", "End"].includes(event.key)) return;
    event.preventDefault();
    const options = [...(menuRef.current?.querySelectorAll<HTMLElement>("[role=option]") ?? [])];
    const current = options.indexOf(document.activeElement as HTMLElement);
    const next = event.key === "Home"
      ? 0
      : event.key === "End"
        ? options.length - 1
        : (current + (event.key === "ArrowDown" ? 1 : -1) + options.length) % options.length;
    options[next]?.focus();
  };

  return (
    <div className={`project-switcher ${open ? "project-switcher--open" : ""}`} ref={rootRef}>
      <button
        className="project-card"
        type="button"
        ref={triggerRef}
        aria-label="Select registered project"
        aria-haspopup="listbox"
        aria-expanded={open}
        aria-controls="registered-projects"
        title={selectedProject?.name}
        onClick={() => setOpen((value) => !value)}
        onKeyDown={(event) => {
          if (event.key === "ArrowDown" || event.key === "ArrowUp") {
            event.preventDefault();
            setOpen(true);
          }
        }}
      >
        <span className="project-card__identity">
          <strong title={selectedProject?.name}>{selectedProject?.name ?? "Local project"}</strong>
          <small title={selectedProject?.path}>{selectedProject?.path ?? fallbackPath ?? "No registered path"}</small>
        </span>
        <ChevronDown className="project-card__chevron" aria-hidden="true" />
      </button>
      {open && (
        <div
          id="registered-projects"
          className="project-menu"
          role="listbox"
          aria-label="Registered projects"
          ref={menuRef}
          onKeyDown={moveOptionFocus}
        >
          {projects.map((project) => {
            const selected = project.id === selectedProject?.id;
            return (
              <button
                key={project.id}
                type="button"
                role="option"
                aria-selected={selected}
                onClick={() => {
                  onSelect(project.id);
                  setOpen(false);
                  triggerRef.current?.focus();
                }}
              >
                <span className="project-menu__identity"><strong>{project.name}</strong><small>{project.path}</small></span>
                {selected && <Check className="project-menu__check" aria-hidden="true" />}
              </button>
            );
          })}
        </div>
      )}
    </div>
  );
}

export default function App() {
  const initialRoute = useMemo(readBrowserRoute, []);
  const [page, setPage] = useState<PageId>(initialRoute.page);
  const [selection, setSelection] = useState<Selection | undefined>(initialRoute.selection);
  const [inspectorTab, setInspectorTab] = useState(initialRoute.inspectorTab ?? "summary");
  const [contextDagId, setContextDagId] = useState<string | undefined>(initialRoute.dagId);
  const [palette, setPalette] = useState(false);
  const [mobileNav, setMobileNav] = useState(false);
  const [sidebarCollapsed, setSidebarCollapsed] = useState(() => localStorage.getItem("dml-sidebar-collapsed") === "true");
  const [projectId, setProjectId] = useState<string | undefined>(initialRoute.projectId);
  const [commitId, setCommitId] = useState<string | undefined>(initialRoute.commitId);
  const [graphFilter, setGraphFilter] = useState<string | undefined>(initialRoute.graphFilter);
  const [selectedDashboard, setSelectedDashboard] = useState<string | undefined>(initialRoute.dashboard);
  const [routeInvalid, setRouteInvalid] = useState(Boolean(initialRoute.invalid));
  const bootstrapGeneration = useRef(0);
  const [theme, setTheme] = useState<"dark" | "light">(() =>
    (localStorage.getItem("dml-theme") as "dark" | "light") ??
    (matchMedia("(prefers-color-scheme: light)").matches ? "light" : "dark"),
  );
  const status = useLoad(api.status);
  const projects = useLoad(api.projects);
  const scope = projectId && commitId ? { project: projectId, revision: commitId } : undefined;
  const overview = useLoad(() => scope ? api.overview(scope.project, scope.revision) : Promise.resolve(undefined), [projectId, commitId]);
  const commits = useLoad<Paginated<Commit>>(() => scope ? api.commits(scope) : Promise.resolve({ items: [] }), [projectId, commitId]);
  const dags = useLoad(() => scope ? api.dags(scope) : Promise.resolve({ items: [] } as DagInventory), [projectId, commitId]);
  const refs = useLoad(() => scope ? api.refs(scope) : Promise.resolve(undefined), [projectId, commitId]);
  const runs = useLoad(() => scope ? api.runs(scope).then((result) => result.items) : Promise.resolve([]), [projectId, commitId]);

  const selectedProjectId = projectId ?? projects.data?.default_project_id;
  const selectedProject = (projects.data?.items ?? []).find((project) => project.id === selectedProjectId);

  const preserveRestoredSelection = useRef(false);
  const applyRoute = useCallback((route: BrowserRoute, restored = false) => {
    preserveRestoredSelection.current = restored;
    setPage(route.page);
    setContextDagId(route.dagId);
    setSelection(route.selection);
    setInspectorTab(route.inspectorTab ?? "summary");
    setProjectId(route.projectId);
    setCommitId(route.commitId);
    setGraphFilter(route.graphFilter);
    setSelectedDashboard(route.dashboard);
    setRouteInvalid(Boolean(route.invalid));
  }, []);

  const navigate = useCallback((nextPage: PageId, options?: { projectId?: string; commitId?: string; dagId?: string; replace?: boolean }) => {
    const nextProject = options?.projectId ?? selectedProjectId;
    const nextCommit = options?.commitId ?? commitId;
    const path = routePath(nextPage, nextProject, nextCommit, options?.dagId);
    window.history[options?.replace ? "replaceState" : "pushState"](null, "", path);
    applyRoute({ page: nextPage, projectId: nextPage === "home" ? undefined : nextProject, commitId: nextPage === "home" || nextPage === "unborn" ? undefined : nextCommit, dagId: options?.dagId });
    setMobileNav(false);
  }, [applyRoute, selectedProjectId, commitId]);

  const changeDashboard = useCallback((name?: string, replace = false) => {
    const params = new URLSearchParams(window.location.search);
    if (name) params.set("dashboard", name); else params.delete("dashboard");
    window.history[replace ? "replaceState" : "pushState"](
      null,
      "",
      `${window.location.pathname}${params.size ? `?${params}` : ""}`,
    );
    setSelectedDashboard(name);
  }, []);

  const navigateHref = useCallback((href: string) => {
    const url = new URL(href, window.location.origin);
    window.history.pushState(null, "", `${url.pathname}${url.search}`);
    applyRoute(readBrowserRoute());
  }, [applyRoute]);

  const openSelection = useCallback((next: Selection, tab = "summary") => {
    const params = new URLSearchParams(window.location.search);
    params.set("resource", next.id);
    params.set("resourceType", next.type);
    params.set("tab", tab);
    window.history.pushState(null, "", `${window.location.pathname}?${params}`);
    setSelection(next);
    setInspectorTab(tab);
  }, []);

  const closeSelection = useCallback(() => {
    const params = new URLSearchParams(window.location.search);
    params.delete("resource");
    params.delete("resourceType");
    params.delete("tab");
    window.history.pushState(null, "", `${window.location.pathname}${params.size ? `?${params}` : ""}`);
    setSelection(undefined);
  }, []);

  const changeInspectorTab = useCallback((tab: string) => {
    const params = new URLSearchParams(window.location.search);
    params.set("tab", tab);
    window.history.replaceState(null, "", `${window.location.pathname}?${params}`);
    setInspectorTab(tab);
  }, []);

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    localStorage.setItem("dml-theme", theme);
  }, [theme]);

  useEffect(() => {
    localStorage.setItem("dml-sidebar-collapsed", String(sidebarCollapsed));
  }, [sidebarCollapsed]);

  useEffect(() => {
    if (page !== "unborn" || !projectId) return;
    let current = true;
    api.overview(projectId, "HEAD").then((result) => {
      if (current && result.revision?.state === "ready" && result.revision.commit) {
        navigate("overview", { projectId, commitId: result.revision.commit, replace: true });
      }
    }).catch(() => undefined);
    return () => { current = false; };
  }, [page, projectId, navigate]);

  const previousScope = useRef<string>();
  useEffect(() => {
    const nextScope = scope ? `${scope.project}:${scope.revision}` : undefined;
    if (previousScope.current && previousScope.current !== nextScope) {
      if (!preserveRestoredSelection.current) {
        closeSelection();
        setContextDagId(undefined);
      }
    }
    previousScope.current = nextScope;
    preserveRestoredSelection.current = false;
  }, [scope?.project, scope?.revision, closeSelection]);

  useEffect(() => {
    const onPopState = () => applyRoute(readBrowserRoute(), true);
    window.addEventListener("popstate", onPopState);
    return () => window.removeEventListener("popstate", onPopState);
  }, [applyRoute, navigate]);

  useEffect(() => {
    const refresh = () => {
      status.reload();
      projects.reload();
      overview.reload();
      commits.reload();
      dags.reload();
      refs.reload();
    };
    return scope ? subscribeToEvents(scope, refresh) : undefined;
  }, [scope?.project, scope?.revision, status.reload, projects.reload, overview.reload, commits.reload, dags.reload, refs.reload]);

  useEffect(() => {
    let awaitingG = false;
    let timer = 0;
    const onKey = (event: globalThis.KeyboardEvent) => {
      const target = event.target;
      const typing = target instanceof Element && target.matches("input, textarea, select, [contenteditable=true]");
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
        event.preventDefault();
        setPalette((value) => !value);
      }
      if (event.key === "Escape") {
        setPalette(false);
        if (!palette && selection) closeSelection();
      }
      if (!typing && event.key.toLowerCase() === "g") {
        awaitingG = true;
        window.clearTimeout(timer);
        timer = window.setTimeout(() => { awaitingG = false; }, 900);
      } else if (!typing && awaitingG) {
        const route = NAV.find((item) => item.shortcut.endsWith(event.key.toUpperCase()));
        if (route) {
          navigate(route.id);
          awaitingG = false;
        }
      }
    };
    window.addEventListener("keydown", onKey);
    return () => {
      window.removeEventListener("keydown", onKey);
      window.clearTimeout(timer);
    };
  }, [palette, selection, closeSelection, navigate]);

  const current = NAV.find((item) => item.id === page) ?? { label: page === "home" ? "Home" : "Overview" };
  const refreshAll = () => {
    status.reload();
    projects.reload();
    overview.reload();
    commits.reload();
    dags.reload(); refs.reload();
    runs.reload();
  };
  const selectProject = async (id: string) => {
    const currentGeneration = ++bootstrapGeneration.current;
    try {
      const result = await api.overview(id, "HEAD");
      if (bootstrapGeneration.current !== currentGeneration) return;
      const revision = result.revision;
      if (revision?.state === "unborn") navigate("unborn", { projectId: id });
      else if (revision?.commit) navigate("overview", { projectId: id, commitId: revision.commit });
    } catch {
      // Keep Home visible; project availability errors are represented by its aggregate data.
    }
  };
  const changeCommit = (nextCommit: string) => {
    const destination = page === "dags" || page === "refs" ? page : "overview";
    navigate(destination, { commitId: nextCommit });
  };
  const projectLive = (status.data?.live_indexes.items ?? []).filter((item) => item.project_id === selectedProjectId);

  return (
    <div className={`app-shell ${sidebarCollapsed ? "app-shell--sidebar-collapsed" : ""} ${selection ? "app-shell--inspecting" : ""}`}>
      <aside id="primary-sidebar" className={`sidebar ${sidebarCollapsed ? "sidebar--collapsed" : ""} ${mobileNav ? "sidebar--open" : ""}`}>
        <div className="brand">
          <img className="brand__mark" src={dagMark} alt="" />
          <a className="brand__home" href="/" onClick={(event) => { event.preventDefault(); navigate("home"); }}><strong>DaggerML</strong><small>Research workbench</small></a>
          <button
            className="icon-button sidebar-toggle"
            onClick={() => setSidebarCollapsed((value) => !value)}
            aria-label={sidebarCollapsed ? "Expand sidebar" : "Collapse sidebar"}
            aria-controls="primary-sidebar"
            aria-expanded={!sidebarCollapsed}
            title={sidebarCollapsed ? "Expand sidebar" : "Collapse sidebar"}
          >
            {sidebarCollapsed ? <PanelLeftOpen /> : <PanelLeftClose />}
          </button>
          <button className="icon-button sidebar__close" onClick={() => setMobileNav(false)} aria-label="Close navigation"><X /></button>
        </div>
        <nav aria-label="Primary navigation">
          {scope && <>
            <p className="nav-label">Project</p>
            {PROJECT_NAV.map((item) => {
              const Icon = item.icon;
              return <button key={item.id} className={`nav-item ${page === item.id ? "nav-item--active" : ""}`} onClick={() => navigate(item.id)} aria-current={page === item.id ? "page" : undefined} aria-label={item.label} title={sidebarCollapsed ? item.label : undefined}><Icon /><span>{item.label}</span><kbd>{item.shortcut}</kbd></button>;
            })}
          </>}
        </nav>
        <div className="sidebar__bottom">
           <ProjectSwitcher
            projects={projects.data?.items ?? []}
            selectedProject={selectedProject}
             fallbackPath={undefined}
            onSelect={selectProject}
          />
        </div>
      </aside>

      <main>
        <header className="topbar">
          <button className="icon-button menu-button" onClick={() => setMobileNav(true)} aria-label="Open navigation"><Menu /></button>
          <div className="breadcrumbs"><span title={selectedProject?.path}>{scope ? selectedProject?.name ?? "Project" : "DaggerML"}</span>{scope && <><b>/</b><span className="breadcrumbs__commit" title={commitId}>{short(commitId)}</span></>}<b>/</b><strong>{current.label}</strong></div>
          <div className="topbar__actions">
            <button className="search-trigger" onClick={() => setPalette(true)}>
              <Search /><span>Search projects, commits, DAGs…</span><kbd>⌘ K</kbd>
            </button>
            <button className="icon-button" onClick={refreshAll} aria-label="Refresh dashboard" title="Refresh"><RefreshCw /></button>
            <button className="icon-button" onClick={() => setTheme(theme === "dark" ? "light" : "dark")} aria-label={`Use ${theme === "dark" ? "light" : "dark"} theme`}>
              {theme === "dark" ? <Sun /> : <Moon />}
            </button>
          </div>
        </header>

        <div className="page">
          {routeInvalid && <Problem title="Page not found" detail="This dashboard location is not available." />}
           {!routeInvalid && page === "home" && <HomePage data={status.data} loading={status.loading} error={status.error} onSelect={openSelection} onProject={selectProject} onProjectsChanged={() => { status.reload(); projects.reload(); }} />}
          {!routeInvalid && page === "unborn" && <PageHeader eyebrow="Project workspace" title={selectedProject?.name ?? projectId ?? "Project"} description="This repository has no commit at HEAD yet." />}
          {!routeInvalid && page === "overview" && <OverviewPage data={overview.data} commits={commits.data?.items ?? []} historyBounded={Boolean(commits.data?.next_cursor)} dags={dags.data?.items ?? []} runs={runs.data ?? []} liveIndexes={projectLive} loading={overview.loading} error={overview.error} select={openSelection} navigate={navigate} projectId={selectedProjectId} onCommit={changeCommit} />}
          {!routeInvalid && page === "dags" && scope && <DagsPage key={`${commitId}:${contextDagId}:${graphFilter ?? ""}`} scope={scope} dags={dags.data?.items ?? []} liveIndexes={dags.data?.live_dags_eligible ? projectLive : []} liveEligible={Boolean(dags.data?.live_dags_eligible)} focusDagId={contextDagId} graphFilter={graphFilter} selectedDashboard={selectedDashboard} onDashboard={changeDashboard} onDagRoute={(id) => navigate("dags", { dagId: id })} loading={dags.loading} error={dags.error} select={openSelection} />}
          {!routeInvalid && page === "refs" && <RefsPage data={refs.data} loading={refs.loading} error={refs.error} onCommit={changeCommit} />}
        </div>
      </main>

      {selection && (scope || selection.type === "index") && <Inspector key={`${scope?.project ?? selection.project_id}:${scope?.revision ?? "current"}:${selection.type}:${selection.id}`} scope={scope} selection={selection} executions={runs.data ?? []} activeTab={inspectorTab} onTab={changeInspectorTab} onNavigateHref={navigateHref} onNavigateDag={(id) => { if (scope) navigate("dags", { dagId: id }); }} onNavigateNode={(id) => openSelection({ type: "node", id }, "value")} onClose={closeSelection} onChanged={refreshAll} />}
      {palette && <CommandPalette onClose={() => setPalette(false)} onNavigate={navigate} onHref={navigateHref} onProject={selectProject} onSelect={openSelection} scope={scope} projects={projects.data?.items ?? []} commits={commits.data?.items ?? []} dags={dags.data?.items ?? []} />}
      <nav className="mobile-destinations" aria-label="Mobile navigation"><button className={page === "home" ? "active" : ""} onClick={() => navigate("home")}><ListTodo />Home</button>{scope && PROJECT_NAV.map((item) => { const Icon = item.icon; return <button key={item.id} className={page === item.id ? "active" : ""} onClick={() => navigate(item.id)} aria-current={page === item.id ? "page" : undefined}><Icon />{item.label}</button>; })}<button onClick={() => setMobileNav(true)} aria-label="Select project"><FolderKanban />Projects</button></nav>
      {mobileNav && <button className="sidebar-scrim" onClick={() => setMobileNav(false)} aria-label="Close navigation" />}
    </div>
  );
}

function PageHeader({ eyebrow, title, description, actions, className = "" }: { eyebrow: string; title: string; description?: string; actions?: ReactNode; className?: string }) {
  return (
    <div className={`page-header ${className}`}>
      <div><p className="eyebrow">{eyebrow}</p><h1>{title}</h1>{description && <p>{description}</p>}</div>
      {actions && <div className="page-header__actions">{actions}</div>}
    </div>
  );
}

function HomePage({ data, loading, error, onSelect, onProject, onProjectsChanged }: {
  data?: StatusPayload;
  loading: boolean;
  error?: string;
  onSelect: (selection: Selection) => void;
  onProject: (id: string) => void;
  onProjectsChanged: () => void;
}) {
  if (loading && !data) return <Loading />;
  if (error && !data) return <Problem title="Status unavailable" detail={error} />;
  const live = data?.live_indexes.items ?? [];
  const commits = data?.recent_commits.items ?? [];
  const groups: Array<{ id: StatusLiveIndex["group"]; title: string; empty: string }> = [
    { id: "needs-attention", title: "Needs attention", empty: "No live work needs attention" },
    { id: "in-progress", title: "In progress", empty: "No live indexes in progress" },
    { id: "canceling", title: "Canceling", empty: "No cancellation is in progress" },
  ];
  if (live.some((item) => item.group === "canceled")) {
    groups.push({ id: "canceled", title: "Canceled", empty: "" });
  }
  return <>
    <div className="page-header page-header--home"><p className="eyebrow">Across registered projects</p></div>
    <div className="status-layout">
      <div className="status-queues">
        {groups.map((group) => {
          const items = live.filter((item) => item.group === group.id);
          return <Panel key={group.id} title={group.title} subtitle={`${items.length} work item${items.length === 1 ? "" : "s"}`}>
            <div className="work-list">{items.map((item) => <StatusIndexRow key={`${item.project_id}-${item.index_ref}`} item={item} onSelect={onSelect} onProject={onProject} />)}{!items.length && <InlineEmpty message={group.empty} />}</div>
          </Panel>;
        })}
      </div>
      <aside className="status-visuals">
        <CommitCalendar commits={commits} retentionDays={data?.retention_days ?? 365} truncated={Boolean(data?.truncated)} />
        {!!data?.diagnostics.length && <Panel title="Availability" subtitle="Failure-isolated project reads"><div className="diagnostic-list">{data.diagnostics.map((item) => <p key={`${item.project_id}-${item.code}`}><AlertTriangle /><span><strong>{item.project_id}</strong><small>{item.message}</small></span></p>)}</div></Panel>}
      </aside>
    </div>
    <ProjectTable projects={data?.projects.items ?? []} onProject={onProject} onChanged={onProjectsChanged} />
  </>;
}

function PathDisclosure({ project }: { project: DashboardProject }) {
  const id = `project-path-${project.id}`;
  const context = project.path_context;
  const visible = context ? `${context.parent}/…/${context.leaf}` : project.path;
  return <span className="path-disclosure" tabIndex={0} aria-describedby={id} data-full={project.path}>{visible}<span id={id} className="sr-only">Full path: {project.path}</span></span>;
}

function ProjectTable({ projects, onProject, onChanged }: { projects: DashboardProject[]; onProject: (id: string) => void; onChanged: () => void }) {
  const [adding, setAdding] = useState(false);
  const [removing, setRemoving] = useState<DashboardProject>();
  const [path, setPath] = useState("");
  const [name, setName] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [mutationError, setMutationError] = useState<string>();
  const addProject = async (event: FormEvent) => {
    event.preventDefault();
    setSubmitting(true);
    setMutationError(undefined);
    try {
      await api.registerProject(path, name.trim() || undefined);
      setAdding(false);
      setPath("");
      setName("");
      onChanged();
    } catch (reason) {
      setMutationError(reason instanceof Error ? reason.message : String(reason));
    } finally {
      setSubmitting(false);
    }
  };
  const removeProject = async () => {
    if (!removing) return;
    setSubmitting(true);
    setMutationError(undefined);
    try {
      await api.unregisterProject(removing.id);
      setRemoving(undefined);
      onChanged();
    } catch (reason) {
      setMutationError(reason instanceof Error ? reason.message : String(reason));
    } finally {
      setSubmitting(false);
    }
  };
  return <Panel className="home-projects" title="Projects" subtitle="Registered repository contexts" action={<button className="text-button project-add" type="button" onClick={() => { setMutationError(undefined); setAdding(true); }}><Plus /> Add project</button>}>
    <div className="project-table" role="table" aria-label="Registered projects">
      <div className="project-table__head" role="row"><span role="columnheader">Project</span><span role="columnheader">Last activity</span><span role="columnheader">Checkout</span><span role="columnheader">Live work</span><span role="columnheader">Sync</span><span role="columnheader">Availability</span><span role="columnheader">Actions</span></div>
      {projects.map((project) => {
        const enabled = project.local_available !== false;
        const activity = project.last_activity;
        return <div key={project.id} role="row" className="project-table__row">
          <button type="button" role="cell" className="project-table__project" disabled={!enabled} onClick={() => onProject(project.id)} aria-label={`${project.name}, ${enabled ? "open project" : "project unavailable"}`}><strong>{project.name}</strong><PathDisclosure project={project} /></button>
          <span role="cell">{activity?.state === "known" && activity.timestamp ? relativeTime(activity.timestamp) : activity?.state ?? "Unknown"}{activity?.truncated && " (bounded)"}</span>
          <span role="cell">{String(project.checkout?.branch ?? project.checkout?.ref ?? project.checkout?.state ?? "Unknown")}</span>
          <span role="cell">{project.live_index_count ?? 0}</span>
          <span role="cell">{String(project.sync?.state ?? "Unknown")}</span>
          <span role="cell"><span className={`availability availability--${project.availability ?? "complete"}`}>{humanize(project.availability ?? "complete")}</span></span>
          <span role="cell"><button type="button" className="project-remove" aria-label={`Remove ${project.name}`} title={`Remove ${project.name}`} onClick={() => { setMutationError(undefined); setRemoving(project); }}><Trash2 /></button></span>
        </div>;
      })}
      {!projects.length && <InlineEmpty message="No projects are registered" />}
    </div>
    {adding && <div className="project-dialog-backdrop" onMouseDown={(event) => event.target === event.currentTarget && setAdding(false)}><section className="project-dialog" role="dialog" aria-modal="true" aria-labelledby="add-project-title"><header><div><p className="eyebrow">Dashboard registration</p><h3 id="add-project-title">Add project</h3></div><button type="button" className="icon-button" aria-label="Close add project" onClick={() => setAdding(false)}><X /></button></header><form onSubmit={addProject}><label><span>Project path</span><input autoFocus required value={path} onChange={(event) => setPath(event.target.value)} placeholder="/path/to/project" /></label><label><span>Display name <small>optional</small></span><input value={name} onChange={(event) => setName(event.target.value)} placeholder="Defaults to directory name" /></label>{mutationError && <p className="form-error" role="alert">{mutationError}</p>}<footer><button type="button" onClick={() => setAdding(false)}>Cancel</button><button className="primary-button" disabled={submitting || !path.trim()}>{submitting ? "Adding…" : "Add project"}</button></footer></form></section></div>}
    {removing && <div className="project-dialog-backdrop"><section className="project-dialog project-dialog--confirm" role="alertdialog" aria-modal="true" aria-labelledby="remove-project-title"><span className="danger-glyph"><Trash2 /></span><h3 id="remove-project-title">Remove {removing.name}?</h3><p>This removes the project from this dashboard only. Repository files and history will not be changed.</p>{mutationError && <p className="form-error" role="alert">{mutationError}</p>}<footer><button type="button" onClick={() => setRemoving(undefined)}>Keep project</button><button type="button" className="danger-button" disabled={submitting} onClick={removeProject}>{submitting ? "Removing…" : "Remove project"}</button></footer></section></div>}
  </Panel>;
}

function StatusIndexRow({ item, onSelect, onProject }: { item: StatusLiveIndex; onSelect: (selection: Selection) => void; onProject: (id: string) => void }) {
  return <article className={`work-item work-item--${item.group}`}>
    <button className="work-item__main" onClick={() => onSelect({ type: "index", id: item.index_ref, project_id: item.project_id, data: item })}>
      <span className="state-mark" aria-hidden="true">{item.group === "needs-attention" ? "!" : item.group === "canceling" ? "↻" : item.group === "canceled" ? "×" : "→"}</span>
      <span><strong>{item.title}</strong><small>{humanize(item.group)} · {relativeTime(item.created_at)}</small>{item.reason && <em>{item.reason}</em>}</span>
    </button>
    <button className="project-link" onClick={() => onProject(item.project_id)}>{item.project_name}</button>
  </article>;
}

function CommitCalendar({ commits, retentionDays, selectedDay, onDay, truncated }: { commits: StatusCommit[]; retentionDays: number; selectedDay?: string; onDay?: (day?: string) => void; truncated: boolean }) {
  const timezone = Intl.DateTimeFormat().resolvedOptions().timeZone;
  const calendar = useMemo(() => {
    const now = new Date();
    now.setHours(12, 0, 0, 0);
    const first = new Date(now);
    first.setDate(first.getDate() - retentionDays);
    const start = new Date(first);
    start.setDate(start.getDate() - start.getDay());
    const end = new Date(now);
    end.setDate(end.getDate() + (6 - end.getDay()));
    const days: Array<{ day: string; inRange: boolean }> = [];
    for (const value = new Date(start); value <= end; value.setDate(value.getDate() + 1)) {
      days.push({ day: localDay(value.toISOString(), timezone), inRange: value >= first && value <= now });
    }
    const weeks = Array.from({ length: Math.ceil(days.length / 7) }, (_, index) => days.slice(index * 7, index * 7 + 7));
    const month = new Intl.DateTimeFormat(undefined, { month: "short", timeZone: timezone });
    const months = weeks.map((week, index) => {
      const visible = week.filter((item) => item.inRange);
      const marker = visible.find((item) => item.day.endsWith("-01")) ?? (index === 0 ? visible[0] : undefined);
      return { index, label: marker ? month.format(new Date(`${marker.day}T12:00:00`)) : "" };
    });
    return { days, weeks, months, firstDay: localDay(first.toISOString(), timezone), lastDay: localDay(now.toISOString(), timezone) };
  }, [retentionDays, timezone]);
  const byDay = new Map<string, StatusCommit[]>();
  commits.forEach((commit) => byDay.set(localDay(commit.timestamp), [...(byDay.get(localDay(commit.timestamp)) ?? []), commit]));
  const max = Math.max(1, ...[...byDay.values()].map((items) => items.length));
  return <Panel title={`${commits.length} commit${commits.length === 1 ? "" : "s"} in the last year`} subtitle={`${timezone} · current-HEAD reachable history`}>
    <div className="contribution-calendar" style={{ "--calendar-weeks": calendar.weeks.length } as CSSProperties}>
      <div className="contribution-calendar__months" aria-label="Months" style={{ gridTemplateColumns: `repeat(${calendar.weeks.length}, minmax(0, 1fr))` }}>{calendar.months.map(({ index, label }) => <span className="calendar-month-label" key={index}>{label}</span>)}</div>
      <div className="contribution-calendar__body">
        <div className="contribution-calendar__weekdays" aria-label="Days of week"><span>Sun</span><span>Mon</span><span>Tue</span><span>Wed</span><span>Thu</span><span>Fri</span><span>Sat</span></div>
        <div className="commit-calendar" role="grid" aria-label={`Commit activity from ${calendar.firstDay} through ${calendar.lastDay} in ${timezone}`} style={{ gridTemplateRows: "repeat(7, 1fr)" }}>{calendar.days.map(({ day, inRange }) => {
      const items = inRange ? byDay.get(day) ?? [] : [];
      const projects = [...new Set(items.map((item) => item.project_name))];
      const level = items.length ? Math.max(1, Math.ceil((items.length / max) * 4)) : 0;
      const label = inRange ? `${day}: ${items.length} commit${items.length === 1 ? "" : "s"}${projects.length ? ` across ${projects.join(", ")}` : ""}${truncated ? "; counts may be truncated" : ""}` : `${day}: outside the one-year window`;
      const className = `contribution-cell contribution-cell--level-${level}${inRange ? "" : " contribution-cell--outside"}`;
      if (!onDay) return <span key={day} role="gridcell" className={className} aria-label={label} title={label} tabIndex={inRange ? 0 : -1} />;
      return <button key={day} role="gridcell" className={className} aria-label={label} aria-pressed={inRange ? selectedDay === day : undefined} title={label} disabled={!inRange} tabIndex={inRange ? 0 : -1} onClick={() => onDay(selectedDay === day ? undefined : day)} />;
    })}</div>
      </div>
      <div className="contribution-calendar__footer"><span>Each square is one day</span><span className="contribution-legend" aria-label="Commit intensity legend">Less<i className="contribution-cell--level-0" /><i className="contribution-cell--level-1" /><i className="contribution-cell--level-2" /><i className="contribution-cell--level-3" /><i className="contribution-cell--level-4" />More</span></div>
    </div>
    {truncated && <p className="calendar-warning"><AlertTriangle /> Counts are truncated by the bounded history scan.</p>}
  </Panel>;
}

function LiveIndexTimeboard({ indexes }: { indexes: StatusLiveIndex[] }) {
  const times = indexes.map((item) => new Date(item.created_at).valueOf()).filter((value) => !Number.isNaN(value));
  const earliest = times.length ? Math.min(...times) : Date.now();
  const now = Date.now();
  const span = Math.max(now - earliest, 1);
  return <div className="live-timeboard" aria-label="Elapsed time for live indexes">
    <div className="live-timeboard__axis" aria-hidden="true"><span>{relativeTime(new Date(earliest).toISOString())}</span><span>now</span></div>
    {indexes.map((item) => {
      const created = new Date(item.created_at).valueOf();
      const width = Number.isNaN(created) ? 0 : Math.max(3, Math.min(100, ((now - created) / span) * 100));
      return <div className="live-timeboard__row" key={item.index_ref}>
        <div className="live-timeboard__label"><code>{item.title || short(item.index_ref, 18)}</code><small>{humanize(item.group)} · {relativeTime(item.created_at)}</small></div>
        <div className="live-timeboard__track"><i className={`live-timeboard__elapsed live-timeboard__elapsed--${item.group}`} style={{ width: `${width}%` }} /></div>
      </div>;
    })}
    {!indexes.length && <InlineEmpty message="No live indexes in this project" />}
  </div>;
}

function LiveIndexList({ indexes, projectId, select }: { indexes: StatusLiveIndex[]; projectId?: string; select: (selection: Selection) => void }) {
  return <div className="overview-live-indexes">
    {indexes.map((item) => <button key={item.index_ref} type="button" onClick={() => select({ type: "index", id: item.index_ref, project_id: projectId, data: item })}>
      <i className={`overview-live-indexes__state overview-live-indexes__state--${item.group}`} aria-hidden="true" />
      <span><strong>{item.title || short(item.index_ref, 18)}</strong><small>{humanize(item.group)} · {relativeTime(item.created_at)}</small></span>
      <span className="overview-live-indexes__open">Open →</span>
    </button>)}
    {!indexes.length && <InlineEmpty message="No live indexes in this project" />}
  </div>;
}

function OverviewPage({ data, commits, historyBounded, dags, runs, liveIndexes, loading, error, select, navigate, projectId, onCommit }: {
  data?: Overview; commits: Commit[]; historyBounded: boolean; dags: Dag[]; runs: Execution[]; liveIndexes: StatusLiveIndex[]; loading: boolean; error?: string; projectId?: string;
  select: (selection: Selection) => void; navigate: (page: PageId, options?: { projectId?: string; dagId?: string }) => void;
  onCommit: (id: string) => void;
}) {
  const selectedCommit = data?.revision?.commit;
  if (loading && !data) return <Loading />;
  if (error && !data) return <Problem title="Could not inspect this directory" detail={error} />;
  if (data?.initialized === false) {
    return <Problem title="No configured DaggerML project" detail={data.message ?? "Register an initialized project in this dashboard configuration directory."} />;
  }
  const active = runs.filter((run) => isActive(run.status));
  return (
    <>
      <header className="overview-scope">
        <div><p className="eyebrow">{data?.revision?.is_current_head === false ? "Repository snapshot · historical commit" : "Repository snapshot"}</p><h1>{data?.project ?? projectId ?? "Project"}<span>/</span><code>{short(selectedCommit ?? data?.head)}</code></h1></div>
        <div className="overview-scope__facts"><span>Checkout <b>{data?.branch ?? "Unknown"}</b></span><span><b>{dags.length}</b> committed DAG{dags.length === 1 ? "" : "s"}</span></div>
      </header>
      <section aria-labelledby="repository-snapshot-heading">
        <div className="overview-section-intro"><h2 id="repository-snapshot-heading">Repository snapshot</h2><p>Immutable data from the selected commit</p></div>
        <div className="overview-snapshot-grid" aria-label="Repository snapshot content">
          <Panel title="Commit history" subtitle="Visible ref tips; select a commit to inspect that snapshot">
          <CommitGraph commits={commits} selectedCommitId={selectedCommit} bounded={historyBounded} onSelect={onCommit} />
          </Panel>
          <Panel title="Committed DAGs" subtitle="Computed research graphs in the selected revision" action={<button className="text-button" onClick={() => navigate("dags")}>Open explorer →</button>}>
          <div className="dag-list">
            {dags.slice(0, 4).map((dag) => (
              <button key={dag.id} onClick={() => select({ type: "dag", id: dag.id, project_id: projectId, data: dag })}>
                <span className="dag-list__icon"><Network /></span><span><strong>{dag.name ?? short(dag.id)}</strong><small>{formatNodeCount(dag)} · {relativeTime(dag.created_at)}</small></span><StatusPill value={dag.status ?? "unknown"} />
              </button>
            ))}
            {!dags.length && <InlineEmpty message="No committed DAGs available" />}
          </div>
          </Panel>
        </div>
      </section>
      <section aria-labelledby="current-operations-heading">
        <div className="overview-current-divider"><div><h2 id="current-operations-heading">Current operations</h2><p>Present state · local work, independent of the selected commit</p></div><p><b>{data?.active_jobs ?? active.length}</b> active jobs <span>·</span> <b>{data?.open_runtimes ?? 0}</b> open runtimes <span>·</span> {data?.executor_status ?? "Executor status unavailable"}</p></div>
        <div className="overview-current-grid">
          <Panel title="Live-index timeboard" subtitle="Elapsed local work; not a completion forecast">
            <LiveIndexTimeboard indexes={liveIndexes} />
          </Panel>
          <Panel title="Active indexes" subtitle={`${liveIndexes.length} current local index${liveIndexes.length === 1 ? "" : "es"}`}>
            <LiveIndexList indexes={liveIndexes} projectId={projectId} select={select} />
          </Panel>
        </div>
      </section>
    </>
  );
}

function RefsPage({ data, loading, error, onCommit }: { data?: RefsEnvelope; loading: boolean; error?: string; onCommit: (id: string) => void }) {
  if (loading && !data) return <Loading />;
  if (error && !data) return <Problem title="Tags and refs unavailable" detail={error} />;
  const selected = data?.selected.commit?.replace(/^commit:/, "");
  return <>
    <PageHeader eyebrow="Current repository topology" title="Tags and refs" description={data?.checkout.branch ? `Checkout: ${data.checkout.branch}` : "Checkout state is unavailable"} />
    <section className="metric-grid" aria-label="Ref summaries">
      <Metric label="Selected commit" value={short(selected)} detail={data?.selected.labels.join(", ") || "No current ref label"} icon={<GitCommitHorizontal />} accent="cyan" />
      <Metric label="Current HEAD" value={short(data?.current_head?.replace(/^commit:/, ""))} detail={data?.checkout.state ?? "Unknown"} icon={<GitBranch />} accent="lime" />
    </section>
    <section className="dashboard-grid">
      <RefSourceSummary sources={data?.sources} />
      <RefSection title="Branches" subtitle="Local, fetched tracking, and live main-remote tips" groups={data?.branches ?? []} selected={selected} onCommit={onCommit} />
      <RefSection title="Tags" subtitle="Tag copies compare by tip equality" groups={data?.tags ?? []} selected={selected} onCommit={onCommit} />
      <Panel className="span-2" title="Dependencies" subtitle={data?.dependencies.truncated ? "Configured dependency list is bounded" : "Import-only dependency refs"}>
        <div className="ref-dependencies">{(data?.dependencies.items ?? []).map((dependency) => {
          const diagnostic = dependency.diagnostic;
          const live = dependency.sources.live;
          return <article key={dependency.name}><header><strong>{dependency.name}</strong>{diagnostic?.availability && <StatusPill value={diagnostic.availability} />}</header><small>{dependency.root ?? diagnostic?.message ?? "Configured dependency"}</small>{diagnostic?.message && dependency.root && <small>{diagnostic.message}</small>}<RefSection title="Branches" subtitle="Fetched and live dependency tips" groups={dependency.branches} selected={selected} onCommit={onCommit} /><RefSection title="Tags" subtitle="Dependency tag copies compare by tip equality" groups={dependency.tags} selected={selected} onCommit={onCommit} />{live?.diagnostic && <RefDiagnostic diagnostic={live.diagnostic} />}</article>;
        })}{!(data?.dependencies.items.length) && <InlineEmpty message="No import-only dependencies are configured" />}</div>
      </Panel>
    </section>
  </>;
}

function RefSourceSummary({ sources }: { sources?: RefsEnvelope["sources"] }) {
  const live = sources?.live;
  const diagnostic = live?.diagnostic;
  const bounded = (["branch", "tag"] as const).filter((kind) => live?.[kind]?.truncated);
  if (!diagnostic && !bounded.length) return null;
  return <Panel className="span-2" title="Main remote" subtitle="Live remote reads are separate from fetched tracking refs">
    {diagnostic && <RefDiagnostic diagnostic={diagnostic} />}
    {bounded.length > 0 && <p className="ref-note">Live {bounded.join(" and ")} refs are bounded; omitted refs are not known absent.</p>}
  </Panel>;
}

function RefDiagnostic({ diagnostic }: { diagnostic: { availability?: string; message?: string } }) {
  return <p className="ref-diagnostic"><StatusPill value={diagnostic.availability ?? "unknown"} /><span>{diagnostic.message ?? "Ref source availability is unknown"}</span></p>;
}

function RefSection({ title, subtitle, groups, selected, onCommit }: { title: string; subtitle: string; groups: RefGroup[]; selected?: string; onCommit: (id: string) => void }) {
  return <Panel title={title} subtitle={subtitle}><div className="ref-groups">{groups.map((group) => <article key={group.name}><header><strong>{group.name}</strong><StatusPill value={group.relation} /></header>{group.upstream && <small>Upstream: {group.upstream}</small>}<RefTips label="Local" tips={group.local ? [{ name: group.name, ...group.local }] : []} selected={selected} onCommit={onCommit} /><RefTips label="Fetched tracking" tips={group.tracking ? [{ name: group.name, ...group.tracking }] : []} selected={selected} onCommit={onCommit} /><RefTips label="Fetched dependency" tips={group.fetched ? [{ name: group.name, ...group.fetched }] : []} selected={selected} onCommit={onCommit} /><RefTips label="Live remote" tips={group.live ? [{ name: group.name, ...group.live }] : []} selected={selected} onCommit={onCommit} /></article>)}{!groups.length && <InlineEmpty message={`No ${title.toLowerCase()} are available`} />}</div></Panel>;
}

function RefTips({ label, tips, selected, onCommit, truncated }: { label: string; tips: unknown[]; selected?: string; onCommit: (id: string) => void; truncated?: boolean }) {
  const entries = tips.filter(isRecord);
  if (!entries.length) return null;
  return <div className="ref-tips"><small>{label}{truncated ? " (bounded)" : ""}</small>{entries.map((tip) => {
    const commit = String(tip.commit ?? "").replace(/^commit:/, "");
    const inspectable = tip.inspectable === true;
    const active = commit === selected;
    const select = () => { if (inspectable) onCommit(commit); };
    return <button key={`${label}-${commit}`} type="button" disabled={!inspectable} aria-current={active ? "true" : undefined} aria-label={`${label} tip ${short(commit)}${inspectable ? ", select revision" : ", not locally available"}`} title={inspectable ? "Select this locally available revision" : "Not locally available; selecting does not fetch remote data"} onClick={select} onKeyDown={(event) => { if (event.key === "Enter" || event.key === " ") { event.preventDefault(); select(); } }}><code>{short(commit, 12)}</code><span>{active ? "Selected" : inspectable ? "Inspect" : "Not locally available"}</span></button>;
  })}</div>;
}

function DagsPage({ scope, dags, liveIndexes, liveEligible, focusDagId, graphFilter, selectedDashboard, onDashboard, onDagRoute, loading, error, select }: { scope: ProjectScope; dags: Dag[]; liveIndexes: StatusLiveIndex[]; liveEligible: boolean; focusDagId?: string; graphFilter?: string; selectedDashboard?: string; onDashboard: (name?: string, replace?: boolean) => void; onDagRoute: (id: string) => void; loading: boolean; error?: string; select: (selection: Selection) => void }) {
  const [activeId, setActiveId] = useState<string | undefined>(focusDagId);
  const [detail, setDetail] = useState<Dag>();
  const [detailError, setDetailError] = useState<string>();
  const [query, setQuery] = useState(graphFilter ?? "");
  const [expanded, setExpanded] = useState(false);
  useEffect(() => { if (focusDagId) setActiveId(focusDagId); }, [focusDagId]);
  useEffect(() => {
    if (!expanded) return;
    const close = (event: globalThis.KeyboardEvent) => { if (event.key === "Escape") setExpanded(false); };
    window.addEventListener("keydown", close);
    return () => window.removeEventListener("keydown", close);
  }, [expanded]);
  const partialDags: Dag[] = liveIndexes.filter((item) => item.dag_ref).map((item) => ({ id: item.dag_ref!, name: item.title, status: item.group, created_at: item.created_at, source_index: item.index_ref } as Dag));
  const inventory = [...partialDags, ...dags.filter((dag) => !partialDags.some((partial) => partial.id === dag.id))];
  const inventorySummary = inventory.find((dag) => dag.id === activeId);
  const summary = inventorySummary ?? (activeId ? { id: activeId } as Dag : inventory[0]);
  useEffect(() => {
    if (!summary) {
      setDetail(undefined);
      setDetailError(undefined);
      return;
    }
    let current = true;
    setDetail(undefined);
    setDetailError(undefined);
    api.dag(scope, summary.id)
      .then((value) => { if (current) setDetail({ ...summary, ...value, name: summary.name ?? value.name }); })
      .catch((reason: unknown) => { if (current) setDetailError(reason instanceof Error ? reason.message : "DAG details are unavailable."); });
    return () => { current = false; };
  }, [scope.project, scope.revision, summary?.id, Boolean(inventorySummary)]);
  const active = detail?.id === summary?.id ? detail : summary;
  const nodes = (active?.nodes ?? []).filter((node) => !query || `${node.label} ${node.kind} ${node.function}`.toLowerCase().includes(query.toLowerCase()));
  const visible = new Set(nodes.map((node) => node.id));
  const edges = (active?.edges ?? inferEdges(nodes)).filter((edge) => visible.has(edge.source) && visible.has(edge.target));
  const chooseDag = (id: string) => {
    setActiveId(id);
    onDagRoute(id);
  };
  const updateQuery = (value: string) => {
    setQuery(value);
    const params = new URLSearchParams(window.location.search);
    if (value) params.set("graphFilter", value); else params.delete("graphFilter");
    window.history.replaceState(null, "", `${window.location.pathname}${params.size ? `?${params}` : ""}`);
  };
  return (
    <>
      <PageHeader eyebrow="Selected repository snapshot" title="DAG Explorer" actions={!expanded ? <><div className="input-with-icon"><Search /><input value={query} onChange={(event) => updateQuery(event.target.value)} placeholder="Filter nodes…" aria-label="Filter DAG nodes" /></div>{query && <button className="text-button" onClick={() => updateQuery("")}>Reset graph</button>}<button type="button" className="icon-button" onClick={() => setExpanded(true)} aria-label="Expand DAG graph" title="Expand DAG graph"><Maximize2 /></button></> : undefined} />
      {loading ? <Loading /> : error ? <Problem title="DAGs unavailable" detail={error} /> : detailError ? <Problem title="DAG not found in this revision" detail={detailError} /> : (<>
        <div className={`dag-workspace ${expanded ? "dag-workspace--expanded" : ""}`}>
          {expanded && <div className="dag-workspace__toolbar">
            <label className="dag-workspace__selector"><span>DAG</span><select value={summary?.id ?? ""} onChange={(event) => chooseDag(event.target.value)} aria-label="Select DAG">
              {summary && !inventory.some((dag) => dag.id === summary.id) && <option value={summary.id}>Function context · {short(summary.id, 18)}</option>}
              {inventory.map((dag) => <option key={dag.id} value={dag.id}>{dag.name ?? short(dag.id)}</option>)}
            </select></label>
            <div className="dag-workspace__actions"><div className="input-with-icon"><Search /><input value={query} onChange={(event) => updateQuery(event.target.value)} placeholder="Filter nodes…" aria-label="Filter expanded DAG nodes" /></div>{query && <button className="text-button" onClick={() => updateQuery("")}>Reset graph</button>}<button type="button" className="icon-button" onClick={() => setExpanded(false)} aria-label="Exit expanded DAG graph" title="Exit expanded DAG graph"><Minimize2 /></button></div>
          </div>}
          <div className="explorer-layout">
            {!expanded && <aside className="dag-picker" aria-label="DAGs">
              <p className="nav-label">DAGs <span>{inventory.length}</span></p>
              {activeId && !inventory.some((dag) => dag.id === activeId) && <button className="active"><span className="dag-picker__icon dag-picker__icon--neutral"><Network /></span><span><strong>Function context</strong><small>{short(activeId, 18)}</small></span></button>}
              {inventory.map((dag) => {
                const displayed = dag.id === active?.id ? active : dag;
                const partial = liveIndexes.find((item) => item.dag_ref === dag.id);
                const outcome = partial ? partialDagOutcome(partial.group) : dagOutcome(displayed?.status);
                return <button key={dag.id} className={dag.id === active?.id ? "active" : ""} onClick={() => chooseDag(dag.id)}><span className={`dag-picker__icon dag-picker__icon--${outcome}`} title={partial ? `Partial DAG: ${humanize(partial.group)}` : `DAG outcome: ${displayed?.status ?? "unknown"}`}><Network /></span><span><strong>{dag.name ?? short(dag.id)}</strong><small>{partial ? `Live index · ${humanize(partial.state ?? partial.group)}` : formatNodeCount(displayed)}</small></span></button>;
              })}
              {!!inventory.length && <div className="dag-outcome-legend" aria-label="DAG outcome legend"><strong>Outcome</strong><span><i className="dag-outcome-legend__mark dag-picker__icon--index" />Active index</span><span><i className="dag-outcome-legend__mark dag-picker__icon--attention" />Waiting for you</span><span><i className="dag-outcome-legend__mark dag-picker__icon--normal" />Normal DAG</span><span><i className="dag-outcome-legend__mark dag-picker__icon--failure" />DAG error</span><span><i className="dag-outcome-legend__mark dag-picker__icon--cancelled" />Cancelled</span></div>}
              {!inventory.length && <InlineEmpty message="No committed or partial DAGs found" />}
            </aside>}
            <div className="dag-canvas">
              {active?.function && <div className="context-banner"><span><span><strong>Function context DAG</strong><small>Runnable and script evidence are available.</small></span></span><button type="button" className="text-button" onClick={() => select({ type: "dag", id: active.id, data: active })}>Inspect Runnable →</button></div>}
              <FlowGraph key={expanded ? "expanded" : "standard"} nodes={nodes} edges={edges} onSelect={select} />
            </div>
          </div>
        </div>
        {!expanded && active?.id && !active.source_index && <CustomDashboardPanel scope={scope} dagId={active.id} selected={selectedDashboard} onSelected={onDashboard} />}
      </>)}
    </>
  );
}

function Inspector({ scope, selection, executions, activeTab, onTab, onNavigateHref, onNavigateDag, onNavigateNode, onClose, onChanged }: { scope?: ProjectScope; selection: Selection; executions: Execution[]; activeTab: string; onTab: (tab: string) => void; onNavigateHref: (href: string) => void; onNavigateDag: (id: string) => void; onNavigateNode: (id: string) => void; onClose: () => void; onChanged: () => void }) {
  const [detail, setDetail] = useState<unknown>(selection.data);
  const [stream, setStream] = useState<"stdout" | "stderr">("stdout");
  const [logs, setLogs] = useState("");
  const [logCursor, setLogCursor] = useState<string>();
  const [hasMoreLogs, setHasMoreLogs] = useState(false);
  const [loading, setLoading] = useState(false);
  const [confirming, setConfirming] = useState(false);
  const [notice, setNotice] = useState("");
  useEffect(() => {
    setDetail(selection.data);
    const currentProject = selection.project_id ?? scope?.project;
    const loader =
      selection.type === "commit" && scope ? api.commit(scope, selection.id) :
      selection.type === "dag" && scope ? api.dag(scope, selection.id) :
      selection.type === "node" && scope ? api.node(scope, selection.id) :
      selection.type === "execution" && scope ? api.execution(scope, selection.id) : Promise.resolve(selection.data);
    const resolvedLoader = selection.type === "index" && currentProject ? api.liveIndex(currentProject, selection.id) : loader;
    resolvedLoader.then(setDetail).catch(() => undefined);
  }, [scope?.project, scope?.revision, selection]);
  const record = isRecord(detail) ? detail : {};
  const functionContext = isRecord(record.function) ? record.function : {};
  const functionDag = isRecord(functionContext.dag) ? functionContext.dag : {};
  const logsExecutionId = selection.type === "execution" || selection.type === "index" ? selection.id : undefined;
  const logsFunctionDagId = !logsExecutionId && typeof functionContext.cache_key === "string" && typeof functionDag.ref === "string" ? functionDag.ref : undefined;
  const logsSourceId = logsExecutionId ?? logsFunctionDagId;
  const logsSource = logsExecutionId ? "execution" : "function-dag";
  useEffect(() => {
    if (activeTab !== "logs" || !logsSourceId || !scope) return;
    let cursor: string | undefined;
    api.logs(scope, logsSourceId, stream, undefined, logsSource).then((result) => {
      cursor = result.next_cursor;
      setLogCursor(result.next_cursor);
      setHasMoreLogs(Boolean(result.has_more));
      setLogs(result.text ?? result.lines?.join("\n") ?? "No log output.");
    }).catch((reason: unknown) => setLogs(reason instanceof Error ? reason.message : String(reason)));
    return subscribeToLogs(scope, logsSourceId, stream, (event) => {
      try {
        const payload = JSON.parse(event.data) as { text?: string; events?: Array<{ timestamp?: number; message?: string }>; next_cursor?: string };
        if (payload.next_cursor === cursor) return;
        cursor = payload.next_cursor;
        setLogCursor(payload.next_cursor);
        const next = payload.text ?? payload.events?.map((item) => `${item.timestamp ? new Date(item.timestamp).toISOString() : ""} ${item.message ?? ""}`.trim()).join("\n") ?? "";
        if (next) setLogs((value) => `${value && value !== "No log output." ? `${value}\n` : ""}${next}`);
      } catch {
        // Ignore malformed transient SSE messages; the next event can recover.
      }
    }, logsSource);
  }, [scope, selection, activeTab, stream, logsSourceId, logsSource]);
  const loadMoreLogs = async () => {
    if (!logCursor || !logsSourceId || !scope) return;
    const result = await api.logs(scope, logsSourceId, stream, logCursor, logsSource);
    const next = result.text ?? result.lines?.join("\n") ?? "";
    if (next) setLogs((value) => `${value}\n${next}`);
    setLogCursor(result.next_cursor);
    setHasMoreLogs(Boolean(result.has_more));
  };
  const availableTabs = [
    "summary",
    ...(selection.type === "node" ? ["value"] : []),
    ...(isRecord(functionContext.runnable) ? ["runnable"] : []),
    ...(selection.type === "dag" || selection.type === "node" || isRecord(record.dag) || isRecord(record.dags) ? ["dag"] : []),
    ...(selection.type === "index" || selection.type === "execution" || Array.isArray(record.lineage) ? ["lineage"] : []),
    ...(logsSourceId ? ["logs"] : []),
    ...(Array.isArray(record.runnable_chain) || "runnable" in record || isRecord(record.evidence) ? ["runnable"] : []),
  ];
  const tab = availableTabs.includes(activeTab) ? activeTab : "summary";
  const inspectorHref = (() => {
    const params = new URLSearchParams(window.location.search);
    params.set("resource", selection.id);
    params.set("resourceType", selection.type);
    params.set("tab", tab);
    return `${window.location.pathname}?${params}`;
  })();
  const selectionData = isRecord(selection.data) ? selection.data : {};
  const selectionLinks = isRecord(selectionData.links) ? selectionData.links : {};
  const detailDag = isRecord(record.dag) ? record.dag : {};
  const dagRef = typeof selectionData.dag_ref === "string" ? selectionData.dag_ref : typeof detailDag.ref === "string" ? detailDag.ref : undefined;
  const dagHref = selection.type === "index"
    ? typeof selectionLinks.dag === "string"
      ? selectionLinks.dag
      : scope && dagRef ? routePath("dags", scope.project, scope.revision, dagRef) : undefined
    : undefined;
  const resourceHref = dagHref ?? inspectorHref;
  const projectHref = selection.project_id ? routePath("unborn", selection.project_id) : undefined;
  const nodeType = selection.type === "node" && typeof record.type === "string" ? record.type : undefined;
  const contextDagRef = nodeType && typeof functionDag.ref === "string" ? functionDag.ref : undefined;
  const followHref = (event: ReactMouseEvent<HTMLAnchorElement>, href: string) => {
    event.preventDefault();
    onNavigateHref(href);
  };
  const title = String(record.title ?? record.name ?? record.label ?? record.message ?? short(selection.id, 18));
  const canCancel = selection.type === "execution" && ["running", "queued", "launching", "pending"].includes(String(record.status ?? "").toLowerCase());
  const cancel = async () => {
    setLoading(true);
    try {
      if (!scope) return;
      const { nonce } = await api.cancelNonce(scope, selection.id);
      await api.cancel(scope, selection.id, nonce);
      setNotice("Cancellation requested. DaggerML will safely drive it to a terminal state.");
      setConfirming(false);
      onChanged();
    } catch (reason) {
      setNotice(reason instanceof Error ? reason.message : String(reason));
    } finally {
      setLoading(false);
    }
  };
  const resizeInspector = (event: ReactPointerEvent<HTMLButtonElement>) => {
    event.preventDefault();
    const move = (pointer: PointerEvent) => {
      const width = Math.max(340, Math.min(680, window.innerWidth - pointer.clientX));
      document.documentElement.style.setProperty("--inspector-width", `${width}px`);
    };
    const stop = () => {
      window.removeEventListener("pointermove", move);
      window.removeEventListener("pointerup", stop);
    };
    window.addEventListener("pointermove", move);
    window.addEventListener("pointerup", stop);
  };
  return (
    <aside className="inspector" aria-label={`${selection.type} inspector`}>
      <button className="inspector__resize" aria-label="Resize inspector" onPointerDown={resizeInspector} />
      <header><div>{contextDagRef ? <div className="inspector__meta-row"><p className="eyebrow inspector__crumbs">{projectHref ? <><a href={projectHref} onClick={(event) => followHref(event, projectHref)}>Project {selection.project_id}</a><span aria-hidden="true"> · </span></> : null}{nodeType ? <span>{nodeType}</span> : <a href={resourceHref} onClick={(event) => followHref(event, resourceHref)}>{selection.type}</a>}</p><button type="button" className="text-button inspector__context-link" onClick={() => onNavigateDag(contextDagRef)}>Open context DAG →</button></div> : <p className="eyebrow inspector__crumbs">{projectHref ? <><a href={projectHref} onClick={(event) => followHref(event, projectHref)}>Project {selection.project_id}</a><span aria-hidden="true"> · </span></> : null}{nodeType ? <span>{nodeType}</span> : <a href={resourceHref} onClick={(event) => followHref(event, resourceHref)}>{selection.type}</a>}</p>}{selection.type !== "node" && <h2><a className="inspector__resource-link" href={resourceHref} onClick={(event) => followHref(event, resourceHref)}>{title}</a></h2>}<button className="copyable-ref" onClick={() => navigator.clipboard.writeText(selection.id)} title="Copy reference"><code>{selection.id}</code><Copy /></button></div><button className="icon-button" onClick={onClose} aria-label="Close inspector"><PanelRightClose /></button></header>
      <div className="inspector__tabs">
        {availableTabs.map((name) => <button key={name} className={tab === name ? "active" : ""} onClick={() => onTab(name)}>{humanize(name)}</button>)}
      </div>
      <div className="inspector__body">
        {tab === "summary" && <DetailView selection={selection} record={record} onNavigateDag={onNavigateDag} />}
        {tab === "value" && <InspectorValue scope={scope} record={record} onNavigateNode={onNavigateNode} />}
        {tab === "runnable" && isRecord(functionContext.runnable) && <RunnableInspectionView scope={scope} value={functionContext.runnable as unknown as RunnableInspection} onNavigateNode={onNavigateNode} />}
        {tab === "dag" && <InspectorDag record={record} onNavigateDag={onNavigateDag} />}
        {tab === "lineage" && <InspectorLineage record={record} executions={executions} />}
        {tab === "logs" && logsSourceId && <LogView value={logs} stream={stream} onStream={setStream} hasMore={hasMoreLogs} onMore={loadMoreLogs} />}
        {tab === "runnable" && <InspectorRunnable record={record} />}
        {notice && <p className="notice" role="status">{notice}</p>}
      </div>
      {canCancel && <footer><button className="danger-button" onClick={() => setConfirming(true)}><CircleStop /> Cancel execution</button></footer>}
      {confirming && <div className="confirm-overlay" role="alertdialog" aria-modal="true" aria-labelledby="cancel-title">
        <div><span className="danger-glyph"><CircleStop /></span><h3 id="cancel-title">Cancel this execution?</h3><p>This requests full cancellation, including relevant downstream execution work.</p><div><button onClick={() => setConfirming(false)}>Keep running</button><button className="danger-button" disabled={loading} onClick={cancel}>{loading ? "Requesting…" : "Cancel execution"}</button></div></div>
      </div>}
    </aside>
  );
}

function InspectorDag({ record, onNavigateDag }: { record: Record<string, unknown>; onNavigateDag: (id: string) => void }) {
  const contained = Object.entries(isRecord(record.dags) ? record.dags : {});
  if (contained.length) {
    return <section className="detail-section"><h3>Contained DAGs</h3><div className="contained-dags">{contained.map(([name, value]) => {
      const ref = String(isRecord(value) ? value.ref ?? value.id ?? "" : value);
      return <button key={`${name}-${ref}`} onClick={() => ref && onNavigateDag(ref)}><Network /><span><strong>{name}</strong><code>{ref}</code></span><b>Open →</b></button>;
    })}</div></section>;
  }
  const dag = isRecord(record.dag) ? record.dag : record;
  const ref = String(dag.ref ?? record.id ?? "");
  return <section className="detail-section"><h3>DAG</h3><dl><div><dt>Reference</dt><dd><code>{ref || "Unavailable"}</code></dd></div>{"partial" in dag && <div><dt>Completeness</dt><dd>{dag.partial ? "Expected partial graph" : "Committed graph"}</dd></div>}</dl>{ref && <button className="text-button" onClick={() => onNavigateDag(ref)}>Open in DAG Explorer →</button>}<pre>{boundedJson(dag)}</pre></section>;
}

function InspectorLineage({ record, executions }: { record: Record<string, unknown>; executions: Execution[] }) {
  const lineage = Array.isArray(record.lineage) ? record.lineage.filter(isRecord) : executions.map((item) => item as unknown as Record<string, unknown>);
  return <section className="detail-section"><h3>Execution lineage</h3><div className="lineage-list">{lineage.map((item) => <article key={String(item.execution_id ?? item.id)} style={{ "--lineage-depth": Math.max(0, Number(item.depth ?? 0)) } as CSSProperties}><span className="state-mark">{item.predates_index ? "↤" : Number(item.depth ?? 0) > 0 ? "↳" : "→"}</span><span><strong>{String(item.execution_id ?? item.id)}</strong><small>{humanize(String(item.lifecycle ?? item.status ?? "unknown"))}{item.predates_index ? " · Predates index" : ""}</small><time>{absoluteInterval(item.created_at ?? item.started_at, item.updated_at)}</time></span></article>)}{!lineage.length && <InlineEmpty message="No linked execution records are available" />}</div></section>;
}

function InspectorRunnable({ record }: { record: Record<string, unknown> }) {
  const chain = Array.isArray(record.runnable_chain) ? record.runnable_chain.filter(isRecord) : [];
  return <section className="detail-section"><h3>Runnable</h3>{chain.length > 0 ? <div className="runnable-chain">{chain.map((item, index) => <RunnableCard key={index} value={item} />)}</div> : <pre>{boundedJson(record.runnable ?? record.evidence ?? record.diagnostics ?? "No runnable evidence is available")}</pre>}</section>;
}

function DetailView({ selection, record, onNavigateDag }: { selection: Selection; record: Record<string, unknown>; onNavigateDag: (id: string) => void }) {
  const hidden = new Set(["value", "value_runnable", "runnable_chain", "nodes", "edges", "children", "parents", "refs", "function", "context_dag", "description"]);
  const entries = Object.entries(record).filter(([key, value]) => !hidden.has(key) && value !== null && value !== undefined && typeof value !== "object");
  const chain = Array.isArray(record.runnable_chain) ? record.runnable_chain.filter(isRecord) : [];
  return (
    <>
      {"status" in record && <div className="inspector-status"><StatusPill value={String(record.status)} /><span>{selection.type === "execution" ? elapsed(String(record.started_at ?? ""), String(record.updated_at ?? "")) : "Persisted state"}</span></div>}
      {selection.type === "execution" && isRecord(record.fndag) && <FndagDetails value={record.fndag} />}
      {isRecord(record.function) && <FunctionContextDetails value={record.function} />}
      <section className="detail-section"><h3>Properties</h3><dl>{entries.map(([key, value]) => <div key={key}><dt>{humanize(key)}</dt><dd>{renderScalar(key, value)}</dd></div>)}</dl></section>
      {chain.length > 0 && <section className="detail-section"><h3>Execution chain</h3><div className="runnable-chain">{chain.map((item, index) => <RunnableCard key={index} value={item} />)}</div></section>}
      {selection.type === "commit" && Array.isArray(record.parents) && <section className="detail-section"><h3>Parent commits</h3>{record.parents.map((parent) => <code className="block-code" key={String(parent)}>{String(parent)}</code>)}</section>}
    </>
  );
}

function FunctionContextDetails({ value }: { value: Record<string, unknown> }) {
  const dag = isRecord(value.dag) ? value.dag : {};
  return <section className="detail-section"><h3>Function context</h3><dl>
    <div><dt>Context DAG</dt><dd><code>{String(dag.ref ?? "Unavailable")}</code></dd></div>
    <div><dt>Cache key</dt><dd><code>{String(value.cache_key ?? "Unavailable")}</code></dd></div>
  </dl></section>;
}

function InspectorValue({ scope, record, onNavigateNode }: { scope?: ProjectScope; record: Record<string, unknown>; onNavigateNode: (id: string) => void }) {
  if (record.value_kind === "runnable" && isRecord(record.value_runnable)) {
    return <RunnableInspectionView scope={scope} value={record.value_runnable as unknown as RunnableInspection} onNavigateNode={onNavigateNode} />;
  }
  return <section className="detail-section"><h3>Persisted value</h3><dl><div><dt>Type</dt><dd>{String(record.value_type ?? "Unknown")}</dd></div><div><dt>Classification</dt><dd>{humanize(String(record.value_kind ?? "value"))}</dd></div></dl><pre>{boundedJson(record.value)}</pre></section>;
}

function RunnableInspectionView({ scope, value, onNavigateNode }: { scope?: ProjectScope; value: RunnableInspection; onNavigateNode: (id: string) => void }) {
  const [script, setScript] = useState(value.script);
  useEffect(() => {
    setScript(value.script);
    if (!scope || value.script?.state !== "available" || !value.script.href) return;
    let active = true;
    api.runnableScript(scope, value.script.href).then((result) => { if (active) setScript({ ...value.script, ...result }); });
    return () => { active = false; };
  }, [scope, value]);
  const layers: Record<string, unknown>[] = [];
  let layer: unknown = value.stack;
  for (let index = 0; index < 16 && isRecord(layer); index += 1) {
    layers.push(layer);
    layer = layer.sub;
  }
  const entrypoint = isRecord(value.entrypoint) ? value.entrypoint : {};
  return <>
    <section className="detail-section"><h3>Runnable stack</h3>{layers.length ? <div className="runnable-chain">{layers.map((item, index) => <RunnableCard key={index} value={item} />)}</div> : <InlineEmpty message={value.diagnostic ?? "Runnable evidence is unavailable"} />}{value.truncated && <p className="diagnostic-note">Runnable evidence was bounded.</p>}</section>
    {Object.keys(entrypoint).length > 0 && <section className="detail-section"><h3>Entrypoint</h3><div className="runnable-chain"><RunnableCard value={entrypoint} /></div></section>}
    <section className="detail-section"><h3>Python script</h3>{typeof script?.source === "string" ? <><p className="script-uri"><code>{script.uri}</code>{script.truncated ? " · bounded preview" : ""}</p><SourceCode value={script.source} /></> : <InlineEmpty message={script?.message ?? (script?.state === "available" ? "Loading script source…" : `Script unavailable: ${humanize(script?.code ?? script?.state ?? "unknown")}`)} />}</section>
    <section className="detail-section"><h3>Prepopulated nodes</h3>{value.prepopulated?.length ? <div className="prepop-table" role="table" aria-label="Prepopulated nodes"><div role="row" className="prepop-table__head"><span role="columnheader">Name</span><span role="columnheader">Type</span><span role="columnheader">Node</span></div>{value.prepopulated.map((item) => <div role="row" key={item.name}><code role="cell">{item.name}</code><span role="cell">{item.type}</span><span role="cell">{item.node?.ref ? <button type="button" className="text-button" onClick={() => onNavigateNode(item.node!.ref!)}>{item.node.ref}</button> : "Not instantiated"}</span></div>)}</div> : <InlineEmpty message="No prepopulated values are declared" />}</section>
  </>;
}

function FndagDetails({ value }: { value: Record<string, unknown> }) {
  const timing = isRecord(value.timing) ? value.timing : {};
  const argv = isRecord(value.argv) ? value.argv : {};
  const output = isRecord(value.output) ? value.output : {};
  const inputs = Array.isArray(argv.inputs) ? argv.inputs.filter(isRecord) : [];
  return <section className="detail-section"><h3>Function DAG</h3><dl>
    <div><dt>Cache key</dt><dd><code>{String(value.cache_key ?? "Unavailable")}</code></dd></div>
    <div><dt>Started</dt><dd>{formatTimestamp(timing.started_at)}</dd></div>
    <div><dt>Ended</dt><dd>{formatTimestamp(timing.ended_at)}</dd></div>
    <div><dt>Duration</dt><dd>{typeof timing.duration_seconds === "number" ? `${timing.duration_seconds.toFixed(2)}s` : "In progress"}</dd></div>
    <div><dt>argv</dt><dd><code>{String(argv.ref ?? "Unavailable")}</code></dd></div>
    <div><dt>Output DAG</dt><dd><code>{String(output.ref ?? "Not committed")}</code></dd></div>
  </dl>{inputs.length > 0 && <div className="link-list"><strong>Inputs</strong>{inputs.map((input, index) => <code key={index}>{String(input.ref ?? boundedJson(input.value))}</code>)}</div>}</section>;
}

function RunnableCard({ value }: { value: Record<string, unknown> }) {
  const kind = String(value.type ?? value.kind ?? "resource").toLowerCase();
  const details = isRecord(value.details) ? value.details : {};
  const safe = Object.entries({ ...value, ...details }).filter(([key, item]) =>
    !/(secret|credential|authorization|token|env(?:ironment)?_?values?)/i.test(key) &&
    !["type", "kind", "source", "details", "sub", "truncated"].includes(key) &&
    item !== undefined && item !== null && typeof item !== "object",
  );
  return <article><span className="runnable-chain__rail"><i /><b /></span><div><p><ResourceIcon kind={kind} /><strong>{humanize(kind)}</strong></p><dl>{safe.slice(0, 12).map(([key, item]) => <div key={key}><dt>{humanize(key)}</dt><dd>{renderScalar(key, item)}</dd></div>)}</dl>{kind === "script" && typeof value.source === "string" && <SourceCode value={value.source} />}</div></article>;
}

function ResourceIcon({ kind }: { kind: string }) {
  return kind.includes("docker") || kind.includes("batch") ? <Box /> : kind.includes("ssh") ? <Server /> : kind.includes("script") ? <TerminalSquare /> : <Archive />;
}

function SourceCode({ value }: { value: string }) {
  const source = truncate(value, 16000);
  return <pre className="source-code" aria-label="Script source">{source.split("\n").map((line, index) => <span className="source-line" key={index}><i>{index + 1}</i><code>{highlightPython(line)}</code></span>)}</pre>;
}

function highlightPython(line: string): ReactNode[] {
  const tokens = line.split(/(\b(?:def|class|return|import|from|as|if|else|elif|for|while|in|try|except|with|yield|await|async|lambda|True|False|None)\b|#[^\n]*|"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'|\b\d+(?:\.\d+)?\b)/g);
  return tokens.filter(Boolean).map((token, index) => {
    const kind = token.startsWith("#") ? "comment" : /^["']/.test(token) ? "string" : /^\d/.test(token) ? "number" : /^(def|class|return|import|from|as|if|else|elif|for|while|in|try|except|with|yield|await|async|lambda|True|False|None)$/.test(token) ? "keyword" : "";
    return kind ? <span className={`syntax-${kind}`} key={index}>{token}</span> : token;
  });
}

function LogView({ value, stream, onStream, hasMore, onMore }: { value: string; stream: "stdout" | "stderr"; onStream: (stream: "stdout" | "stderr") => void; hasMore: boolean; onMore: () => void }) {
  const ref = useRef<HTMLPreElement>(null);
  useEffect(() => {
    if (typeof ref.current?.scrollTo === "function") ref.current.scrollTo({ top: ref.current.scrollHeight });
  }, [value]);
  return <div className="log-view"><header><span><span className="pulse" /> Streaming output</span><span className="log-streams"><button type="button" className={stream === "stdout" ? "active" : ""} onClick={() => onStream("stdout")}>stdout</button><button type="button" className={stream === "stderr" ? "active" : ""} onClick={() => onStream("stderr")}>stderr</button><button type="button" onClick={() => navigator.clipboard.writeText(value)}><Copy /> Copy</button></span></header><pre ref={ref}>{value || "Waiting for output…"}</pre>{hasMore && <button type="button" className="load-more" onClick={onMore}>Load next log segment</button>}</div>;
}

function CommandPalette({ onClose, onNavigate, onHref, onProject, onSelect, scope, projects, commits, dags }: { onClose: () => void; onNavigate: (page: PageId, options?: { projectId?: string }) => void; onHref: (href: string) => void; onProject: (id: string) => void; onSelect: (selection: Selection) => void; scope?: ProjectScope; projects: DashboardProject[]; commits: Commit[]; dags: Dag[] }) {
  type PaletteItem = { type: string; id: string; label: string; detail?: string; project_id?: string; href?: string };
  const [query, setQuery] = useState("");
  const [remoteItems, setRemoteItems] = useState<PaletteItem[]>([]);
  const input = useRef<HTMLInputElement>(null);
  useEffect(() => input.current?.focus(), []);
  useEffect(() => {
    if (!query.trim()) { setRemoteItems([]); return; }
    const timer = window.setTimeout(() => api.search(query, scope).then((result) => setRemoteItems((result.items ?? []).map((item) => ({ ...item, label: item.label ?? item.id })))).catch(() => setRemoteItems([])), 120);
    return () => window.clearTimeout(timer);
  }, [query, scope?.project, scope?.revision]);
  const items = useMemo(() => {
    const q = query.toLowerCase();
    const local: PaletteItem[] = [
      ...NAV.map((item) => ({ type: "page", id: item.id, label: `Go to ${item.label}`, detail: item.shortcut })),
      ...projects.map((item) => ({ type: "project", id: item.id, label: item.name, detail: item.path, project_id: item.id })),
      ...commits.map((item) => ({ type: "commit", id: item.id, label: item.message ?? short(item.id), detail: short(item.id) })),
      ...dags.map((item) => ({ type: "dag", id: item.id, label: item.name ?? short(item.id), detail: `${item.nodes?.length ?? 0} nodes` })),
      ...remoteItems.map((item) => ({ ...item, label: item.label || item.id })),
    ];
    return local.filter((item) => `${item.label} ${item.detail} ${item.id}`.toLowerCase().includes(q)).filter((item, index, all) => all.findIndex((candidate) => candidate.type === item.type && candidate.id === item.id && candidate.project_id === item.project_id) === index).slice(0, 12);
  }, [query, projects, commits, dags, remoteItems]);
  const choose = (item: (typeof items)[number]) => {
    if (item.href) onHref(item.href);
    else if (item.type === "page") onNavigate(item.id as PageId);
    else if (item.type === "project") onProject(item.id);
    else onSelect({ type: item.type as Selection["type"], id: item.id, project_id: item.project_id });
    onClose();
  };
  return <div className="palette-backdrop" onMouseDown={(event) => event.target === event.currentTarget && onClose()}><section className="palette" role="dialog" aria-modal="true" aria-label="Command palette"><div className="palette__input"><Search /><input ref={input} value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Search projects, refs, commits, and DAGs…" onKeyDown={(event) => { if (event.key === "Enter" && items[0]) choose(items[0]); }} /><kbd>ESC</kbd></div><div className="palette__results"><p className="nav-label">{query ? "Matches" : "Quick navigation"}</p>{items.map((item, index) => <button key={`${item.type}-${item.project_id ?? "global"}-${item.id}`} onClick={() => choose(item)}><span className="result-icon">{item.type === "page" ? <Command /> : item.type === "project" ? <FolderKanban /> : item.type === "commit" ? <GitCommitHorizontal /> : item.type === "dag" ? <Network /> : <Activity />}</span><span><strong>{item.label}</strong><small>{item.project_id ? `${item.type} · ${item.project_id} · ${item.detail}` : `${item.type} · ${item.detail}`}</small></span>{index === 0 && <kbd>↵</kbd>}</button>)}{!items.length && <InlineEmpty message="No results" />}</div><footer><span><kbd>↑</kbd><kbd>↓</kbd> navigate</span><span><kbd>↵</kbd> open</span></footer></section></div>;
}

function Metric({ label, value, detail, icon, accent }: { label: string; value: string; detail: string; icon: ReactNode; accent: string }) {
  return <article className={`metric metric--${accent}`}><span className="metric__icon">{icon}</span><div><p>{label}</p><strong>{value}</strong><small>{detail}</small></div></article>;
}
function Panel({ title, subtitle, action, className = "", children }: { title: string; subtitle: string; action?: ReactNode; className?: string; children: ReactNode }) {
  return <section className={`panel ${className}`}><header><div><h2>{title}</h2><p>{subtitle}</p></div>{action}</header>{children}</section>;
}
function RunRow({ run, onClick }: { run: Execution; onClick: () => void }) {
  return <button onClick={onClick} className="run-row"><span className="run-row__icon"><Activity /></span><span><strong>{run.name ?? short(run.id)}</strong><small>{run.executor ?? "local"} · {elapsed(run.started_at, run.updated_at)}</small><i><b style={{ width: `${normalizeProgress(run.progress)}%` }} /></i></span><StatusPill value={run.status} /></button>;
}
function Health({ icon, label, detail, status }: { icon: ReactNode; label: string; detail: string; status: string }) {
  return <div><span className="health-icon">{icon}</span><span><strong>{label}</strong><small>{detail}</small></span><StatusPill value={status} /></div>;
}
function Loading() {
  return <div className="loading" role="status"><span /><span /><span /><p>Inspecting project state…</p></div>;
}
function Problem({ title, detail }: { title: string; detail: string }) {
  return <div className="problem"><span><AlertTriangle /></span><h2>{title}</h2><p>{detail}</p></div>;
}
function InlineEmpty({ message }: { message: string }) {
  return <div className="inline-empty"><span>◇</span>{message}</div>;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
function short(value?: string, length = 8) {
  return value ? value.slice(0, length) : "Unavailable";
}
function truncate(value: string, length: number) {
  return value.length > length ? `${value.slice(0, length)}\n… ${value.length - length} more characters` : value;
}
function boundedJson(value: unknown) {
  return truncate(JSON.stringify(value, null, 2) ?? String(value), 8000);
}
function humanize(value: string) {
  return value.replaceAll("_", " ").replaceAll("-", " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
}
function renderScalar(key: string, value: unknown) {
  const text = String(value);
  if (/(url|uri)/i.test(key)) return sanitizeUri(text);
  return truncate(text, 500);
}
function sanitizeUri(value?: string) {
  if (!value) return "Not configured";
  try {
    const url = new URL(value);
    url.username = "";
    url.password = "";
    url.search = "";
    url.hash = "";
    return url.toString();
  } catch {
    return value.split("?")[0];
  }
}
function count(value?: number | string[]) {
  return Array.isArray(value) ? value.length : value ?? 0;
}
function formatNodeCount(dag?: Dag) {
  const value = dag?.node_count ?? dag?.nodes?.length;
  return value === undefined ? "Node count unavailable" : `${value} node${value === 1 ? "" : "s"}`;
}
function dagOutcome(status?: string) {
  const value = status?.toLowerCase();
  if (["ready", "success", "succeeded", "completed"].includes(value ?? "")) return "normal";
  if (["error", "failed"].includes(value ?? "")) return "failure";
  if (["cancelled", "canceled"].includes(value ?? "")) return "cancelled";
  if (["running", "pending", "queued", "launching", "in-progress", "canceling"].includes(value ?? "")) return "index";
  return "neutral";
}
function partialDagOutcome(group: StatusLiveIndex["group"]) {
  if (group === "in-progress") return "index";
  if (group === "needs-attention" || group === "canceling") return "attention";
  return "cancelled";
}
function normalizeProgress(value?: number) {
  if (value === undefined) return 0;
  return Math.round(Math.min(100, value <= 1 ? value * 100 : value));
}
function isActive(status?: string) {
  return ["running", "queued", "launching", "pending", "cancelling", "cancel-requested", "cancel-ready"].includes(status?.toLowerCase() ?? "");
}
function formatDivergence(ahead = 0, behind = 0) {
  if (!ahead && !behind) return "Up to date";
  return `↑ ${ahead} ahead · ↓ ${behind} behind`;
}
function relativeTime(value?: string) {
  if (!value) return "unknown time";
  const timestamp = new Date(value).valueOf();
  if (Number.isNaN(timestamp)) return value;
  const seconds = Math.round((timestamp - Date.now()) / 1000);
  const formatter = new Intl.RelativeTimeFormat(undefined, { numeric: "auto" });
  if (Math.abs(seconds) < 60) return formatter.format(seconds, "second");
  const minutes = Math.round(seconds / 60);
  if (Math.abs(minutes) < 60) return formatter.format(minutes, "minute");
  const hours = Math.round(minutes / 60);
  if (Math.abs(hours) < 24) return formatter.format(hours, "hour");
  return formatter.format(Math.round(hours / 24), "day");
}
export function localDay(value: string, timezone = Intl.DateTimeFormat().resolvedOptions().timeZone) {
  const date = new Date(value);
  if (Number.isNaN(date.valueOf())) return value.slice(0, 10);
  const parts = new Intl.DateTimeFormat("en-US", { timeZone: timezone, year: "numeric", month: "2-digit", day: "2-digit" }).formatToParts(date);
  const part = (type: string) => parts.find((item) => item.type === type)?.value ?? "";
  return `${part("year")}-${part("month")}-${part("day")}`;
}
function absoluteInterval(start: unknown, end: unknown) {
  const render = (value: unknown) => {
    if (typeof value === "number") return new Date(value < 10_000_000_000 ? value * 1000 : value).toLocaleString();
    if (typeof value === "string" && value) return new Date(value).toLocaleString();
    return "Unknown";
  };
  return `${render(start)} → ${end ? render(end) : "Open"}`;
}
function formatTimestamp(value: unknown) {
  if (typeof value !== "number") return "Unavailable";
  return new Date(value * 1000).toLocaleString();
}
function elapsed(start?: string, end?: string) {
  if (!start) return "—";
  const from = new Date(start).valueOf();
  const to = end ? new Date(end).valueOf() : Date.now();
  if (Number.isNaN(from) || Number.isNaN(to)) return "—";
  const seconds = Math.max(0, Math.round((to - from) / 1000));
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
  return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
}
function inferEdges(nodes: GraphNode[]): GraphEdge[] {
  return nodes.flatMap((node) => (node.inputs ?? []).map((source) => ({ source, target: node.id })));
}
