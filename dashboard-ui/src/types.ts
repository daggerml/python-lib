export type PageId = "home" | "overview" | "dags" | "refs" | "unborn";
export interface ProjectScope {
  project: string;
  revision: string;
}
export type Availability = "complete" | "partial" | "unavailable" | "unauthorized" | "unconfigured";
export type LiveStateGroup = "needs-attention" | "in-progress" | "canceling" | "canceled";

export interface RefLabel {
  name: string;
  kind?: "head" | "branch" | "tag" | "remote";
}

export interface Commit {
  id: string;
  short_id?: string;
  message?: string;
  author?: string;
  timestamp?: string;
  parents?: string[];
  refs?: Array<RefLabel | string>;
  dag_count?: number;
  status?: string;
}

export interface GraphNode {
  id: string;
  label?: string;
  kind?: string;
  status?: string;
  value?: unknown;
  function?: string | FunctionContext;
  executor?: string;
  duration_ms?: number;
  inputs?: string[];
  context_dag?: ResourceLink;
  role?: "argv" | "result" | "error" | "intermediate";
}

export interface GraphEdge {
  id?: string;
  source: string;
  target: string;
  label?: string;
}

export interface Dag {
  id: string;
  name?: string;
  node_count?: number;
  status?: string;
  nodes?: GraphNode[];
  edges?: GraphEdge[];
  commit_id?: string;
  created_at?: string;
  function?: FunctionContext;
  tags?: string[];
  source_index?: string;
}

export interface CustomDashboardDefinition {
  name: string;
  tags: string[];
  eager: boolean;
}

export interface CustomDashboardInventory extends Paginated<CustomDashboardDefinition> {
  default?: string | null;
  diagnostics?: Array<{ entry_point: string; code: string; message: string }>;
}

export interface PlotlyDashboardResult {
  kind: "plotly";
  data: Array<Record<string, unknown>>;
  layout: Record<string, unknown>;
  config: Record<string, unknown>;
  cache_hit: boolean;
}

export interface VegaLiteDashboardResult {
  kind: "vega-lite";
  spec: Record<string, unknown>;
  cache_hit: boolean;
}

export type CustomDashboardResult = PlotlyDashboardResult | VegaLiteDashboardResult;

export interface ResourceLink {
  ref?: string;
  href?: string;
}

export interface FunctionContext {
  dag?: ResourceLink;
  argv?: ResourceLink;
  cache_key?: string;
  runnable?: RunnableInspection;
}

export interface RunnableLayer {
  kind?: string;
  target?: string;
  adapter?: string;
  details?: Record<string, unknown>;
  sub?: RunnableLayer;
  truncated?: boolean;
}

export interface ScriptEvidence {
  state: string;
  message?: string;
  uri?: string;
  href?: string;
  source?: string;
  truncated?: boolean;
  code?: string;
}

export interface PrepopulatedValue {
  name: string;
  type: string;
  node?: ResourceLink | null;
}

export interface RunnableInspection {
  state: string;
  stack?: RunnableLayer;
  entrypoint?: RunnableLayer;
  script?: ScriptEvidence;
  prepopulated?: PrepopulatedValue[];
  truncated?: boolean;
  diagnostic?: string;
}

export interface Execution {
  id: string;
  name?: string;
  status?: string;
  executor?: string;
  progress?: number;
  started_at?: string;
  updated_at?: string;
  parent_id?: string;
  children?: string[];
  cache_key?: string;
  runnable_chain?: Runnable[];
}

export interface Runnable {
  type?: string;
  kind?: string;
  [key: string]: unknown;
}

export interface Remote {
  name: string;
  url?: string;
  status?: string;
  branches?: number | string[];
  tags?: number | string[];
  ahead?: number;
  behind?: number;
  latency_ms?: number;
}

export interface Overview {
  initialized?: boolean;
  project?: string;
  project_home?: string;
  branch?: string;
  head?: string;
  ahead?: number;
  behind?: number;
  active_jobs?: number;
  open_runtimes?: number;
  remote_status?: string;
  executor_status?: string;
  recent_commits?: Commit[];
  recent_dags?: Dag[];
  message?: string;
  revision?: {
    requested: string;
    state: "ready" | "unborn";
    commit?: string;
    current_head?: string;
    is_current_head: boolean;
  };
  repository?: Record<string, unknown>;
  current?: Record<string, unknown>;
}

export interface DagInventory extends Paginated<Dag> {
  revision?: Overview["revision"];
  live_dags_eligible?: boolean;
}

export interface RefTip {
  commit: string;
  inspectable: boolean;
}

export interface RefDiagnostic {
  availability?: Availability | "unknown";
  message?: string;
}

export interface RefSource {
  truncated?: boolean;
  diagnostic?: RefDiagnostic;
}

export interface RefSourceKinds {
  branch?: RefSource;
  tag?: RefSource;
  diagnostic?: RefDiagnostic;
}

export interface RefGroup {
  kind: "branch" | "tag";
  name: string;
  local?: RefTip;
  fetched?: RefTip;
  tracking?: RefTip;
  live?: RefTip;
  upstream?: string;
  relation: string;
}

export interface RefsEnvelope {
  revision: NonNullable<Overview["revision"]>;
  checkout: { mode?: string; branch?: string; state?: string };
  current_head?: string;
  selected: { commit?: string; labels: string[] };
  branches: RefGroup[];
  tags: RefGroup[];
  sources: Record<string, RefSourceKinds>;
  dependencies: { items: DependencyRefGroup[]; truncated: boolean };
}

export interface DependencyRefGroup {
  name: string;
  root?: string;
  diagnostic?: RefDiagnostic;
  branches: RefGroup[];
  tags: RefGroup[];
  sources: Record<string, RefSourceKinds>;
}

export interface DashboardProject {
  id: string;
  path: string;
  name: string;
  registered_at?: number;
  availability?: Availability;
  live_index_count?: number;
  recent_commit_count?: number;
  commit_truncated?: boolean;
  local_available?: boolean;
  path_context?: { parent: string; leaf: string };
  current_head?: string;
  checkout?: { state?: string; branch?: string; ref?: string; [key: string]: unknown };
  sync?: { state?: string; [key: string]: unknown };
  last_activity?: { state: "known" | "unknown" | "unavailable"; timestamp?: string; source?: string; truncated?: boolean };
}

export interface EvidenceLinks {
  project?: string;
  inspector?: string;
  dag?: string;
  history?: string;
}

export interface StatusLiveIndex {
  project_id: string;
  project_name: string;
  index_ref: string;
  title: string;
  group: LiveStateGroup;
  created_at: string;
  reason?: string;
  state?: string;
  dag_ref?: string;
  links: EvidenceLinks;
}

export interface StatusCommit {
  project_id: string;
  project_name: string;
  commit_ref: string;
  message: string;
  author: string;
  timestamp: string;
  refs: string[];
  dag_count: number;
  error_dag_count: number;
  links: EvidenceLinks;
}

export interface StatusDiagnostic {
  project_id: string;
  availability: Availability;
  code: string;
  message: string;
  retryable: boolean;
}

export interface StatusPayload {
  generated_at: string;
  retention_days: number;
  projects: Paginated<DashboardProject>;
  live_indexes: Paginated<StatusLiveIndex>;
  recent_commits: Paginated<StatusCommit>;
  diagnostics: StatusDiagnostic[];
  truncated: boolean;
}

export interface TimelineRecord {
  execution_id: string;
  lifecycle: string;
  created_at?: string | null;
  updated_at?: string | null;
  timing: "predates-index" | "recorded" | "open";
  predates_index: boolean;
  children: string[];
  spawned: string[];
  parent_execution_id?: string | null;
  depth?: number;
}

export interface LiveIndexDetail {
  index_ref: string;
  title: string;
  state: string;
  group: LiveStateGroup;
  created_at: string;
  reason?: string;
  dag: ResourceLink & { partial?: boolean };
  execution?: Record<string, unknown> | null;
  lineage: TimelineRecord[];
  evidence: Record<string, unknown>;
  identifiers: Record<string, string>;
  diagnostics: StatusDiagnostic[];
}

export interface Fndag {
  execution?: Record<string, unknown>;
  runtime?: Record<string, unknown>;
  cache_key?: string;
  argv?: { ref?: string; href?: string; inputs?: Array<Record<string, unknown>> };
  output?: { ref?: string; href?: string } | null;
  timing?: { started_at?: number; ended_at?: number; duration_seconds?: number };
  runnable?: unknown;
  script?: { href?: string };
  logs?: Record<string, { href?: string }>;
}

export interface Paginated<T> {
  items: T[];
  next_cursor?: string | null;
  total?: number;
}

export interface Selection {
  type: "commit" | "dag" | "node" | "execution" | "remote" | "index";
  id: string;
  project_id?: string;
  data?: unknown;
}
