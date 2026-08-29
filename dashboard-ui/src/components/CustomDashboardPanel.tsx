import { useEffect, useRef, useState } from "react";
import { RefreshCw } from "lucide-react";
import { api } from "../api";
import type { CustomDashboardInventory, CustomDashboardResult, PlotlyDashboardResult, ProjectScope, VegaLiteDashboardResult } from "../types";

export function CustomDashboardPanel({ scope, dagId, selected, onSelected }: {
  scope: ProjectScope;
  dagId: string;
  selected?: string;
  onSelected: (name?: string, replace?: boolean) => void;
}) {
  const [inventory, setInventory] = useState<CustomDashboardInventory>();
  const [result, setResult] = useState<CustomDashboardResult>();
  const [error, setError] = useState<string>();
  const [loading, setLoading] = useState(true);
  const generation = useRef(0);

  useEffect(() => {
    const current = ++generation.current;
    setInventory(undefined);
    setResult(undefined);
    setError(undefined);
    setLoading(true);
    api.customDashboards(scope, dagId)
      .then((value) => { if (generation.current === current) setInventory(value); })
      .catch((reason: unknown) => { if (generation.current === current) setError(message(reason)); })
      .finally(() => { if (generation.current === current) setLoading(false); });
  }, [scope.project, scope.revision, dagId]);

  useEffect(() => {
    if (!selected && inventory?.default) onSelected(inventory.default, true);
  }, [inventory?.default, selected, onSelected]);

  const compatible = inventory?.items.some((item) => item.name === selected);
  useEffect(() => {
    if (!selected || !compatible) {
      setResult(undefined);
      return;
    }
    const current = ++generation.current;
    setResult(undefined);
    setError(undefined);
    setLoading(true);
    api.customDashboard(scope, dagId, selected)
      .then((value) => { if (generation.current === current) setResult(value); })
      .catch((reason: unknown) => { if (generation.current === current) setError(message(reason)); })
      .finally(() => { if (generation.current === current) setLoading(false); });
  }, [scope.project, scope.revision, dagId, selected, compatible]);

  const refresh = async () => {
    if (!selected) return;
    const current = ++generation.current;
    setLoading(true);
    setError(undefined);
    try {
      const value = await api.refreshCustomDashboard(scope, dagId, selected);
      if (generation.current === current) setResult(value);
    } catch (reason) {
      if (generation.current === current) setError(message(reason));
    } finally {
      if (generation.current === current) setLoading(false);
    }
  };

  if (!loading && !error && !inventory?.items.length) return null;
  return <section className="custom-dashboard" aria-label="Custom dashboard">
    <header>
      <div><p className="eyebrow">Plugin visualization</p><h2>Custom dashboard</h2></div>
      <div className="custom-dashboard__actions">
        <label><span>Dashboard</span><select aria-label="Select custom dashboard" value={selected ?? ""} onChange={(event) => onSelected(event.target.value || undefined)}>
          <option value="">Choose dashboard…</option>
          {(inventory?.items ?? []).map((item) => <option key={item.name} value={item.name}>{item.name}{item.eager ? " · eager" : ""}</option>)}
        </select></label>
        {selected && compatible && <button className="icon-button" onClick={refresh} disabled={loading} aria-label="Refresh custom dashboard" title="Refresh custom dashboard"><RefreshCw /></button>}
      </div>
    </header>
    {loading && <p className="custom-dashboard__state">Loading custom dashboard…</p>}
    {!loading && error && <div className="custom-dashboard__state custom-dashboard__state--error" role="alert"><strong>Dashboard unavailable</strong><span>{error}</span></div>}
    {!loading && selected && inventory && !compatible && <div className="custom-dashboard__state custom-dashboard__state--error" role="alert"><strong>Dashboard unavailable</strong><span>This dashboard is not compatible with the selected DAG.</span></div>}
    {!loading && !selected && <p className="custom-dashboard__state">Choose a compatible dashboard to run it.</p>}
    {!loading && result?.kind === "plotly" && <PlotlyRenderer result={result} />}
    {!loading && result?.kind === "vega-lite" && <VegaLiteRenderer result={result} />}
    {result && <small className="custom-dashboard__cache">{result.cache_hit ? "Loaded from local cache" : "Rendered now"}</small>}
  </section>;
}

function PlotlyRenderer({ result }: { result: PlotlyDashboardResult }) {
  const root = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const target = root.current;
    if (!target) return;
    let active = true;
    let dispose: (() => void) | undefined;
    import("plotly.js-dist-min").then(({ default: Plotly }) => {
      if (!active) return;
      void Plotly.newPlot(target, result.data, { ...result.layout, autosize: true }, { responsive: true, ...result.config });
      dispose = () => Plotly.purge(target);
    });
    return () => { active = false; dispose?.(); };
  }, [result]);
  return <div ref={root} className="custom-dashboard__renderer" aria-label="Plotly dashboard" />;
}

function VegaLiteRenderer({ result }: { result: VegaLiteDashboardResult }) {
  const root = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const target = root.current;
    if (!target) return;
    let active = true;
    let view: { finalize: () => void } | undefined;
    import("vega-embed").then(({ default: embed }) => embed(target, result.spec, { actions: false })).then((value) => {
      if (active) view = value.view;
      else value.view.finalize();
    });
    return () => { active = false; view?.finalize(); };
  }, [result]);
  return <div ref={root} className="custom-dashboard__renderer" aria-label="Vega-Lite dashboard" />;
}

function message(reason: unknown): string {
  return reason instanceof Error ? reason.message : String(reason);
}
