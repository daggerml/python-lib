// Run with PLAYWRIGHT_MODULE, CHROMIUM_PATH, and BROWSER_OUTPUT pointing to
// existing build-only tooling and a separate evidence directory.
import assert from "node:assert/strict";
import { mkdir, readFile } from "node:fs/promises";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { createServer } from "vite";

const { chromium } = await import(pathToFileURL(process.env.PLAYWRIGHT_MODULE).href);
const output = process.env.BROWSER_OUTPUT;
assert.ok(output, "BROWSER_OUTPUT must name an evidence directory");
await mkdir(output, { recursive: true });
const pages = [
  { id: "extend/executors", title: "Executors", fragment: "fragments/extend/executors.html", order: 13, headings: [] },
  { id: "use/sharing", title: "Sharing", fragment: "fragments/use/sharing.html", order: 10, headings: [] },
  { id: "start-here", title: "DaggerML", fragment: "fragments/start-here.html", order: 0, headings: [] },
  { id: "extend/codecs", title: "Codecs", fragment: "fragments/extend/codecs.html", order: 11, headings: [] },
  { id: "use/projects", title: "Projects", fragment: "fragments/use/projects.html", order: 5, headings: [{ id: "configure", level: 2, text: "Configure" }] },
  { id: "start-here/dags", title: "DAGs", fragment: "fragments/start-here/dags.html", order: 2, headings: [{ id: "source", level: 2, text: "Source" }] },
  { id: "use/artifacts", title: "Artifacts", fragment: "fragments/use/artifacts.html", order: 6, headings: [] },
  { id: "extend/adapters", title: "Adapters", fragment: "fragments/extend/adapters.html", order: 12, headings: [{ id: "transport", level: 2, text: "Transport" }] },
  { id: "use/execution", title: "Execution", fragment: "fragments/use/execution.html", order: 7, headings: [] },
  { id: "use/inspection", title: "Inspection", fragment: "fragments/use/inspection.html", order: 8, headings: [] },
  { id: "use/runtimes", title: "Runtimes", fragment: "fragments/use/runtimes.html", order: 9, headings: [] },
];
const script = 'print("verified example")\n';
// A valid empty ZIP tests binary download handling independently of packaging.
const bundle = Buffer.from("504b0506000000000000000000000000000000000000", "hex");
const fixtures = new Map([
  ["/docs/static/manifest.json", { contentType: "application/json", body: JSON.stringify({ pages }) }],
  ["/docs/static/fragments/start-here.html", { contentType: "text/html", body: '<h1>Documentation</h1><p>Use, extend, and develop DaggerML.</p><a href="/docs/start-here/dags">DAGs</a>' }],
  ["/docs/static/fragments/start-here/dags.html", { contentType: "text/html", body: '<h1>DAGs</h1><p>A canonical script with matching downloads.</p><h2 id="source">Source</h2><pre><code>print("verified example")</code></pre><a href="/docs/static/downloads/one.py" download>Download script</a><p><a href="/docs/static/downloads/one.zip" download>Download bundle</a></p><a href="/docs/start-here">Documentation home</a>' }],
  ["/docs/static/fragments/use/projects.html", { contentType: "text/html", body: '<h1>Projects</h1><p>Continue the executable research course.</p><h2 id="configure">Configure</h2><p>Inspect and configure one durable project.</p>' }],
  ["/docs/static/fragments/extend/adapters.html", { contentType: "text/html", body: '<h1>Adapters</h1><p>Follow delayed authoring to an executable transport.</p><h2 id="transport">Transport</h2><p>Move one operation across the boundary.</p>' }],
  ["/docs/static/downloads/one.py", { contentType: "text/x-python", filename: "one.py", body: script }],
  ["/docs/static/downloads/one.zip", { contentType: "application/zip", filename: "one.zip", body: bundle }],
]);
const server = await createServer({
  plugins: [{
    name: "docs-browser-fixtures",
    configureServer(vite) {
      vite.middlewares.use((request, response, next) => {
        const fixture = fixtures.get(new URL(request.url ?? "/", "http://localhost").pathname);
        if (!fixture) return next();
        response.statusCode = 200;
        response.setHeader("Content-Type", fixture.contentType);
        if (fixture.filename) response.setHeader("Content-Disposition", `attachment; filename="${fixture.filename}"`);
        response.end(fixture.body);
      });
    },
  }],
  server: { host: "127.0.0.1", port: 0, open: false },
});
let browser;
try {
  await server.listen();
  browser = await chromium.launch({
    executablePath: process.env.CHROMIUM_PATH,
    headless: true,
    chromiumSandbox: false,
    args: ["--disable-gpu", "--renderer-process-limit=1"],
    timeout: 20000,
  });
  for (const viewport of [{ width: 1440, height: 1000 }, { width: 390, height: 844 }]) {
    for (const theme of ["dark", "light"]) {
      const context = await browser.newContext({ viewport, acceptDownloads: true, colorScheme: "dark" });
      try {
        const page = await context.newPage();
        page.setDefaultTimeout(10000);
        const errors = [];
        page.on("pageerror", (error) => errors.push(error.message));
        await context.route("**/api/**", (route) => {
          const path = new URL(route.request().url()).pathname;
          if (path.endsWith("/events")) return route.fulfill({ contentType: "text/event-stream", body: ": ready\n\n" });
          return route.fulfill({ json: path.endsWith("/status")
            ? { projects: { items: [] }, live_indexes: { items: [] }, recent_commits: { items: [] }, diagnostics: [], retention_days: 365, truncated: false }
            : { items: [] } });
        });
        await page.goto(`${server.resolvedUrls.local[0]}docs/start-here/dags`);
        await page.getByRole("heading", { name: "DAGs" }).waitFor();
        if (theme === "light") await page.getByRole("button", { name: "Use light theme" }).click();
        assert.equal(await page.locator("html").getAttribute("data-theme"), theme);

        async function tabTo(locator) {
          for (let i = 0; i < 40; i++) {
            await page.keyboard.press("Tab");
            if (await locator.evaluate((element) => element === document.activeElement)) {
              assert.equal(await locator.evaluate((element) => element.matches(":focus-visible")), true);
              return;
            }
          }
          assert.fail(`Keyboard could not reach ${await locator.textContent()}`);
        }

        const navigation = page.getByLabel("Documentation navigation");
        const current = navigation.getByRole("button", { name: "DAGs" });
        assert.equal(await current.getAttribute("aria-current"), "page");
        await tabTo(navigation.getByRole("button", { name: "DaggerML", exact: true }));
        await page.keyboard.press("Enter");
        await page.getByRole("heading", { name: "Documentation", exact: true }).waitFor();
        assert.equal(await current.getAttribute("aria-current"), null);
        await tabTo(current);
        await page.keyboard.press("Space");
        await page.getByRole("heading", { name: "DAGs" }).waitFor();
        assert.equal(await current.getAttribute("aria-current"), "page");
        for (const [name, filename, bytes] of [["Download script", "one.py", Buffer.from(script)], ["Download bundle", "one.zip", bundle]]) {
          await tabTo(page.getByRole("link", { name }));
          const downloadReady = page.waitForEvent("download");
          await page.keyboard.press("Enter");
          const download = await downloadReady;
          assert.equal(download.suggestedFilename(), filename);
          assert.deepEqual(await readFile(await download.path()), bytes);
          assert.equal(new URL(page.url()).pathname, "/docs/start-here/dags");
        }
        const globalNav = page.getByRole("navigation", { name: viewport.width < 600 ? "Mobile navigation" : "Primary navigation", exact: true });
        assert.equal(await globalNav.getByRole("button", { name: "Docs", exact: true }).getAttribute("aria-current"), "page");
        assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true, "Layout must not overflow the viewport");
        const evidence = `${viewport.width}-${theme}`;

        const use = navigation.locator("details").filter({ has: page.locator("summary", { hasText: "Use" }) });
        await use.locator("summary").click();
        assert.deepEqual(await use.getByRole("button").allTextContents(), ["Projects", "Artifacts", "Execution", "Inspection", "Runtimes", "Sharing"]);
        await use.getByRole("button", { name: "Projects" }).click();
        await page.getByRole("heading", { name: "Projects", exact: true }).waitFor();
        assert.equal(await page.getByRole("button", { name: "Projects", exact: true }).getAttribute("aria-current"), "page");
        assert.equal(await page.getByLabel("On this page").getByRole("button", { name: "Configure" }).isVisible(), true);
        assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true, "Use layout must not overflow the viewport");
        await page.screenshot({ path: join(output, `${evidence}-use.png`), fullPage: true });

        const extend = navigation.locator("details").filter({ has: page.locator("summary", { hasText: "Extend" }) });
        await extend.locator("summary").click();
        assert.deepEqual(await extend.getByRole("button").allTextContents(), ["Codecs", "Adapters", "Executors"]);
        await extend.getByRole("button", { name: "Adapters" }).click();
        await page.getByRole("heading", { name: "Adapters", exact: true }).waitFor();
        assert.equal(await page.getByLabel("On this page").getByRole("button", { name: "Transport" }).isVisible(), true);
        assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true, "Extend layout must not overflow the viewport");
        await page.screenshot({ path: join(output, `${evidence}-extend.png`), fullPage: true });
        await tabTo(globalNav.getByRole("button", { name: "Home", exact: true }));
        await page.keyboard.press("Enter");
        assert.equal(new URL(page.url()).pathname, "/");
        assert.deepEqual(errors, []);
        console.log(`PASS ${evidence}: keyboard Enter/Space, focus-visible, current page, script/ZIP bytes, Home, no horizontal overflow or page errors`);
      } finally {
        await context.close();
      }
    }
  }
} finally {
  await browser?.close();
  await server.close();
}
