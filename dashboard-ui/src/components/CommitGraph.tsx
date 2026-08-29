import { curveBumpY, line } from "d3";
import { useEffect, useMemo, useRef, useState, type KeyboardEvent, type WheelEvent } from "react";
import type { Commit, RefLabel } from "../types";

const ROW = 72;
const LANE = 28;
const LEFT = 38;

interface PositionedCommit extends Commit {
  x: number;
  y: number;
  lane: number;
}

function refLabel(ref: string | RefLabel): RefLabel {
  if (typeof ref === "string") return { name: ref, kind: ref === "HEAD" ? "head" : "branch" };
  return ref;
}

export function CommitGraph({
  commits,
  selectedCommitId,
  bounded = false,
  onSelect,
}: {
  commits: Commit[];
  selectedCommitId?: string;
  bounded?: boolean;
  onSelect: (id: string) => void;
}) {
  const [scale, setScale] = useState(1);
  const [offset, setOffset] = useState(0);
  const [hovered, setHovered] = useState<PositionedCommit | null>(null);
  const surface = useRef<SVGSVGElement>(null);
  const positioned = useMemo(() => {
    const lanes = new Map<string, number>();
    let nextLane = 0;
    return commits.map((commit, index) => {
      const assigned = lanes.get(commit.id);
      const lane = assigned ?? nextLane++;
      (commit.parents ?? []).forEach((parent, parentIndex) => {
        if (!lanes.has(parent)) lanes.set(parent, parentIndex === 0 ? lane : nextLane++);
      });
      return { ...commit, lane, x: LEFT + lane * LANE, y: 38 + index * ROW };
    });
  }, [commits]);
  const lookup = useMemo(() => new Map(positioned.map((commit) => [commit.id, commit])), [positioned]);
  useEffect(() => {
    [...(surface.current?.querySelectorAll<SVGGElement>("[data-commit]") ?? [])]
      .find((element) => element.dataset.commitId === selectedCommitId)?.focus();
  }, [selectedCommitId]);
  const path = line<[number, number]>().x((point) => point[0]).y((point) => point[1]).curve(curveBumpY);
  const width = Math.max(860, 420 + Math.max(1, ...positioned.map((commit) => commit.lane)) * LANE);
  const height = Math.max(280, positioned.length * ROW + 46);

  const onWheel = (event: WheelEvent<SVGSVGElement>) => {
    event.preventDefault();
    if (event.ctrlKey || event.metaKey) {
      setScale((value) => Math.max(0.65, Math.min(1.6, value - event.deltaY * 0.002)));
    } else {
      setOffset((value) => Math.max(-height / 2, Math.min(0, value - event.deltaY * 0.6)));
    }
  };

  const onKey = (event: KeyboardEvent<SVGGElement>, index: number) => {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      onSelect(positioned[index].id);
    }
    const next = event.key === "ArrowDown" ? index + 1 : event.key === "ArrowUp" ? index - 1 : -1;
    if (next >= 0 && next < positioned.length) {
      event.preventDefault();
      surface.current?.querySelectorAll<SVGGElement>("[data-commit]")[next]?.focus();
    }
  };

  if (!commits.length) return <EmptyGraph label="No commits in this view" />;

  return (
    <div className="graph-surface commit-graph">
      <div className="graph-tools">
        <button onClick={() => setScale((value) => Math.min(1.6, value + 0.1))} aria-label="Zoom in">+</button>
        <button onClick={() => setScale((value) => Math.max(0.65, value - 0.1))} aria-label="Zoom out">−</button>
        <button onClick={() => { setScale(1); setOffset(0); }}>Reset</button>
      </div>
      <svg ref={surface} width="100%" height={Math.min(640, height)} viewBox={`0 0 ${width} ${Math.min(640, height)}`} onWheel={onWheel}>
        <defs>
          <filter id="nodeGlow" x="-100%" y="-100%" width="300%" height="300%">
            <feGaussianBlur stdDeviation="3" result="blur" />
            <feMerge><feMergeNode in="blur" /><feMergeNode in="SourceGraphic" /></feMerge>
          </filter>
        </defs>
        <g transform={`translate(0 ${offset}) scale(${scale})`}>
          {positioned.flatMap((commit) =>
            (commit.parents ?? []).map((parent) => {
              const target = lookup.get(parent);
              if (!target) return null;
              return (
                <path
                  className="commit-edge"
                  key={`${commit.id}-${parent}`}
                  d={path([[commit.x, commit.y], [target.x, target.y]]) ?? ""}
                  fill="none"
                />
              );
            }),
          )}
          {positioned.map((commit, index) => (
            <g
              className={`commit-node ${commit.id === selectedCommitId ? "commit-node--selected" : ""}`}
              data-commit
              data-commit-id={commit.id}
              key={commit.id}
              tabIndex={0}
              role="button"
              aria-current={commit.id === selectedCommitId ? "true" : undefined}
              aria-label={`${commit.message ?? "Commit"} ${commit.short_id ?? commit.id.slice(0, 8)}${commit.id === selectedCommitId ? ", selected" : ""}`}
              onKeyDown={(event) => onKey(event, index)}
              onClick={() => onSelect(commit.id)}
              onMouseEnter={() => setHovered(commit)}
              onMouseLeave={() => setHovered(null)}
            >
              <circle cx={commit.x} cy={commit.y} r="7.5" />
              <circle className="commit-node__core" cx={commit.x} cy={commit.y} r="3" />
              <text className="commit-message" x={Math.max(150, commit.x + 26)} y={commit.y - 4}>
                {commit.message || "Untitled commit"}
              </text>
              <text className="commit-meta" x={Math.max(150, commit.x + 26)} y={commit.y + 17}>
                {(commit.short_id ?? commit.id.slice(0, 8))} · {formatTime(commit.timestamp)}
              </text>
              {(commit.refs ?? []).slice(0, 4).map((rawRef, refIndex) => {
                const ref = refLabel(rawRef);
                const label = ref.kind === "head" ? `HEAD → ${ref.name}` : ref.name;
                const width = Math.max(64, Math.min(146, 18 + label.length * 6));
                return <g key={`${ref.kind ?? "branch"}:${ref.name}`} className={`ref-badge-group ref-badge-group--${ref.kind ?? "branch"}`} transform={`translate(${Math.max(455, commit.x + 330) + refIndex * 150}, ${commit.y - 15})`}>
                  <rect className="ref-badge" width={width} height="23" rx="7" />
                  <text className="ref-badge__text" x="9" y="16">{label.slice(0, 20)}</text>
                </g>
              })}
            </g>
          ))}
        </g>
      </svg>
      {hovered && (
        <div className="graph-tooltip" role="tooltip">
          <strong>{hovered.message || "Untitled commit"}</strong>
          <span>{hovered.author || "Unknown author"} · {formatTime(hovered.timestamp)}</span>
          <span>{hovered.parents?.length ?? 0} parent(s) · {hovered.dag_count ?? 0} DAG(s)</span>
        </div>
      )}
      <p className="graph-hint">{bounded ? "Visible tips are bounded · " : ""}Scroll to move · ⌘/Ctrl + scroll to zoom · arrows to navigate</p>
    </div>
  );
}

function formatTime(value?: string) {
  if (!value) return "time unavailable";
  const date = new Date(value);
  return Number.isNaN(date.valueOf()) ? value : new Intl.DateTimeFormat(undefined, { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" }).format(date);
}

function EmptyGraph({ label }: { label: string }) {
  return <div className="empty-graph"><span className="empty-graph__glyph">⌁</span><p>{label}</p></div>;
}
