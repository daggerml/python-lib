import { useEffect, useState } from "react";
import { Moon, Sun } from "lucide-react";
import { DocsPage } from "./App";
import { BrandIcon } from "./components/BrandIcon";

function currentPage() {
  return window.location.pathname.replace(/^\/docs\/?/, "").replace(/\/$/, "") || undefined;
}

export function DocumentationSite() {
  const [page, setPage] = useState(currentPage);
  const [theme, setTheme] = useState<"dark" | "light">(() =>
    localStorage.getItem("dml-theme") === "light" ? "light" : "dark",
  );
  useEffect(() => {
    const update = () => setPage(currentPage());
    window.addEventListener("popstate", update);
    return () => window.removeEventListener("popstate", update);
  }, []);
  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    localStorage.setItem("dml-theme", theme);
  }, [theme]);

  const navigate = (id?: string, hash = "") => {
    const next = id?.replace(/\/$/, "");
    const path = !next || next === "start-here" ? "/" : `/docs/${next}/`;
    window.history.pushState({}, "", path + hash);
    setPage(currentPage());
    if (hash) document.getElementById(decodeURIComponent(hash.slice(1)))?.scrollIntoView?.();
    else window.scrollTo(0, 0);
  };

  return <div className="docs-site">
    <header className="docs-site__header">
      <a href="/" className="docs-site__brand"><BrandIcon />DaggerML</a>
      <a href="/api/index.html">API reference</a>
      <button className="icon-button" onClick={() => setTheme(theme === "dark" ? "light" : "dark")} aria-label={`Use ${theme === "dark" ? "light" : "dark"} theme`}>
        {theme === "dark" ? <Sun /> : <Moon />}
      </button>
    </header>
    <main className="docs-site__content"><DocsPage pageId={page} theme={theme} onNavigate={navigate} /></main>
  </div>;
}
