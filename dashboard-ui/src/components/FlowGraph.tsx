import { useMemo } from "react";
import {
  Background,
  Controls,
  Handle,
  MarkerType,
  Panel,
  Position,
  ReactFlow,
  type Edge,
  type Node,
  type NodeProps,
} from "@xyflow/react";
import type { GraphEdge, GraphNode, Selection } from "../types";
import { StatusPill } from "./StatusPill";

type FlowData = Record<string, unknown> & { item: GraphNode; kind: "node" | "execution" };

const NODE_WIDTH = 280;
const NODE_HEIGHT = 126;
const LAYER_GAP = 92;
const ROW_GAP = 42;

/**
 * Place a dependency graph in stable left-to-right layers.  The rank of each
 * node is its longest dependency path; ordering each layer by the barycenter
 * of its incoming neighbours keeps large DAGs readable without pretending a
 * general graph is a chronological list.
 */
export function layeredLayout(items: GraphNode[], graphEdges: GraphEdge[]): Map<string, { x: number; y: number }> {
  const known = new Set(items.map((item) => item.id));
  const incoming = new Map(items.map((item) => [item.id, [] as string[]]));
  const outgoing = new Map(items.map((item) => [item.id, [] as string[]]));
  for (const edge of graphEdges) {
    if (!known.has(edge.source) || !known.has(edge.target) || edge.source === edge.target) continue;
    incoming.get(edge.target)!.push(edge.source);
    outgoing.get(edge.source)!.push(edge.target);
  }
  const order = new Map(items.map((item, index) => [item.id, index]));
  const byStableOrder = (a: string, b: string) => (order.get(a)! - order.get(b)!);
  const indegree = new Map([...incoming].map(([id, values]) => [id, values.length]));
  const rank = new Map(items.map((item) => [item.id, 0]));
  const queue = items.filter((item) => !indegree.get(item.id)).map((item) => item.id).sort(byStableOrder);
  const visited = new Set<string>();
  while (queue.length) {
    const id = queue.shift()!;
    visited.add(id);
    for (const target of outgoing.get(id) ?? []) {
      rank.set(target, Math.max(rank.get(target) ?? 0, (rank.get(id) ?? 0) + 1));
      indegree.set(target, (indegree.get(target) ?? 1) - 1);
      if (indegree.get(target) === 0) queue.push(target);
    }
    queue.sort(byStableOrder);
  }
  // Preserve every node even if malformed provenance introduces a cycle.
  for (const item of items) if (!visited.has(item.id)) rank.set(item.id, 0);

  const layers = new Map<number, string[]>();
  for (const item of items) {
    const layer = rank.get(item.id) ?? 0;
    layers.set(layer, [...(layers.get(layer) ?? []), item.id]);
  }
  const layerIndexes = [...layers.keys()].sort((a, b) => a - b);
  const positionInLayer = new Map<string, number>();
  for (const layer of layerIndexes) {
    const ids = layers.get(layer)!;
    ids.sort((a, b) => {
      const predecessors = (incoming.get(a) ?? []).filter((id) => positionInLayer.has(id));
      const predecessorB = (incoming.get(b) ?? []).filter((id) => positionInLayer.has(id));
      const center = (values: string[], fallback: string) => values.length
        ? values.reduce((sum, id) => sum + (positionInLayer.get(id) ?? 0), 0) / values.length
        : order.get(fallback)!;
      return center(predecessors, a) - center(predecessorB, b) || byStableOrder(a, b);
    });
    ids.forEach((id, index) => positionInLayer.set(id, index));
  }
  const tallest = Math.max(...[...layers.values()].map((layer) => layer.length), 1);
  const result = new Map<string, { x: number; y: number }>();
  for (const layer of layerIndexes) {
    const ids = layers.get(layer)!;
    const offset = ((tallest - ids.length) * (NODE_HEIGHT + ROW_GAP)) / 2;
    ids.forEach((id, index) => result.set(id, {
      x: layer * (NODE_WIDTH + LAYER_GAP),
      y: offset + index * (NODE_HEIGHT + ROW_GAP),
    }));
  }
  return result;
}

function ResearchNode({ data, selected }: NodeProps<Node<FlowData>>) {
  const item = data.item;
  const glyph = item.kind === "error" ? "!" : item.kind === "fn" ? "ƒ" : item.kind === "import" ? "↗" : item.kind === "argv" ? "…" : item.kind === "literal" ? "◇" : data.kind === "execution" ? "▶" : "•";
  const type = item.kind ?? data.kind;
  const role = item.role ?? "intermediate";
  return (
    <div className={`flow-node flow-node--type-${type} flow-node--role-${role} ${selected ? "flow-node--selected" : ""}`} aria-label={`${item.label ?? item.id}, ${type} node, ${role} role`}>
      <Handle type="target" position={Position.Left} />
      <div className="flow-node__header">
        <span className={`flow-node__icon flow-node__icon--${item.kind ?? "node"}`}>{glyph}</span>
        <span>{type}</span>
        {data.kind === "execution" && <StatusPill value={item.status ?? "unknown"} />}
      </div>
      <strong>{item.label ?? (typeof item.function === "string" ? item.function : undefined) ?? item.id.slice(0, 14)}</strong>
      <small>{item.executor ?? item.id.slice(0, 18)}</small>
      <Handle type="source" position={Position.Right} />
    </div>
  );
}

export function FlowGraph({
  nodes,
  edges,
  kind = "node",
  onSelect,
}: {
  nodes: GraphNode[];
  edges: GraphEdge[];
  kind?: "node" | "execution";
  onSelect: (selection: Selection) => void;
}) {
  const flowNodes = useMemo<Node<FlowData>[]>(
    () => {
      const positions = layeredLayout(nodes, edges);
      return nodes.map((item) => ({
        id: item.id,
        type: "research",
        data: { item, kind },
        position: positions.get(item.id) ?? { x: 0, y: 0 },
      }));
    },
    [nodes, edges, kind],
  );
  const flowEdges = useMemo<Edge[]>(
    () =>
      edges.map((edge, index) => ({
        id: edge.id ?? `${edge.source}-${edge.target}-${index}`,
        source: edge.source,
        target: edge.target,
        // Edge labels overlap nodes quickly in dense graphs. Node inspection
        // retains edge provenance without making the canvas unreadable.
        type: "smoothstep",
        markerEnd: { type: MarkerType.ArrowClosed, width: 16, height: 16 },
      })),
    [edges],
  );

  if (!nodes.length) return <div className="empty-graph"><span className="empty-graph__glyph">◇</span><p>No graph data is available</p></div>;

  return (
    <div className="flow-wrap" aria-label={`${kind} graph`}>
      <ReactFlow
        nodes={flowNodes}
        edges={flowEdges}
        nodeTypes={{ research: ResearchNode }}
        fitView
        fitViewOptions={{ padding: 0.14 }}
        minZoom={0.12}
        maxZoom={1.8}
        onNodeClick={(_, node) => onSelect({ type: kind, id: node.id, data: node.data.item })}
        nodesConnectable={false}
        proOptions={{ hideAttribution: true }}
      >
        <Background gap={22} size={1} />
        {kind === "node" && <Panel position="top-left" className="graph-legend" aria-label="DAG graph legend">
          <div className="graph-legend__group"><strong>Role</strong><span><i className="legend-role legend-role--argv" />Arguments</span><span><i className="legend-role legend-role--result" />Result</span><span><i className="legend-role legend-role--error" />Error</span><span><i className="legend-role legend-role--intermediate" />Intermediate</span></div>
          <div className="graph-legend__group"><strong>Type</strong><span><i className="legend-shape legend-shape--fn">ƒ</i>Function</span><span><i className="legend-shape legend-shape--literal">◇</i>Literal</span><span><i className="legend-shape legend-shape--import">↗</i>Import</span><span><i className="legend-shape legend-shape--argv">…</i>Argv</span></div>
        </Panel>}
        <Controls showInteractive={false} />
      </ReactFlow>
    </div>
  );
}
