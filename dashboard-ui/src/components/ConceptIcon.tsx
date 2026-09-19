import type { SVGProps } from "react";
import dag16 from "../assets/icons/concepts/16/dag.svg?raw";
import dag20 from "../assets/icons/concepts/20/dag.svg?raw";
import dag24 from "../assets/icons/concepts/24/dag.svg?raw";
import node16 from "../assets/icons/concepts/16/node.svg?raw";
import node20 from "../assets/icons/concepts/20/node.svg?raw";
import node24 from "../assets/icons/concepts/24/node.svg?raw";
import run16 from "../assets/icons/concepts/16/run.svg?raw";
import run20 from "../assets/icons/concepts/20/run.svg?raw";
import run24 from "../assets/icons/concepts/24/run.svg?raw";
import function16 from "../assets/icons/concepts/16/function.svg?raw";
import function20 from "../assets/icons/concepts/20/function.svg?raw";
import function24 from "../assets/icons/concepts/24/function.svg?raw";
import data16 from "../assets/icons/concepts/16/data.svg?raw";
import data20 from "../assets/icons/concepts/20/data.svg?raw";
import data24 from "../assets/icons/concepts/24/data.svg?raw";
import cache16 from "../assets/icons/concepts/16/cache.svg?raw";
import cache20 from "../assets/icons/concepts/20/cache.svg?raw";
import cache24 from "../assets/icons/concepts/24/cache.svg?raw";
import commit16 from "../assets/icons/concepts/16/commit.svg?raw";
import commit20 from "../assets/icons/concepts/20/commit.svg?raw";
import commit24 from "../assets/icons/concepts/24/commit.svg?raw";
import remote16 from "../assets/icons/concepts/16/remote.svg?raw";
import remote20 from "../assets/icons/concepts/20/remote.svg?raw";
import remote24 from "../assets/icons/concepts/24/remote.svg?raw";

type Concept = "dag" | "node" | "run" | "function" | "data" | "cache" | "commit" | "remote";
type IconProps = Omit<SVGProps<SVGSVGElement>, "width" | "height" | "children" | "dangerouslySetInnerHTML"> & { size?: 16 | 20 | 24 };

// Only trusted, bundled artwork is inlined; API content never enters this map.
const artwork = {
  dag: { 16: dag16, 20: dag20, 24: dag24 },
  node: { 16: node16, 20: node20, 24: node24 },
  run: { 16: run16, 20: run20, 24: run24 },
  function: { 16: function16, 20: function20, 24: function24 },
  data: { 16: data16, 20: data20, 24: data24 },
  cache: { 16: cache16, 20: cache20, 24: cache24 },
  commit: { 16: commit16, 20: commit20, 24: commit24 },
  remote: { 16: remote16, 20: remote20, 24: remote24 },
};

export function ConceptIcon({ concept, size = 16, ...props }: IconProps & { concept: Concept }) {
  const source = artwork[concept][size];
  const body = source.slice(source.indexOf(">") + 1, source.lastIndexOf("</svg>"));
  return <svg {...props} className={`concept-icon ${props.className ?? ""}`} width={size} height={size} style={{ ...props.style, width: size, height: size }} viewBox={`0 0 ${size} ${size}`} aria-hidden="true" focusable="false" dangerouslySetInnerHTML={{ __html: body }} />;
}

export function DagIcon(props: IconProps) { return <ConceptIcon concept="dag" {...props} />; }

export function NodeIcon(props: IconProps) { return <ConceptIcon concept="node" {...props} />; }

export function RunIcon(props: IconProps) { return <ConceptIcon concept="run" {...props} />; }

export function FunctionIcon(props: IconProps) { return <ConceptIcon concept="function" {...props} />; }

export function DataIcon(props: IconProps) { return <ConceptIcon concept="data" {...props} />; }

export function CacheIcon(props: IconProps) { return <ConceptIcon concept="cache" {...props} />; }

export function CommitIcon(props: IconProps) { return <ConceptIcon concept="commit" {...props} />; }

export function RemoteIcon(props: IconProps) { return <ConceptIcon concept="remote" {...props} />; }
