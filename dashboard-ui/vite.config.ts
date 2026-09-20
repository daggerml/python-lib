import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig(({ mode }) => ({
  plugins: [react()],
  base: "/",
  build: {
    outDir: "../src/daggerml/dashboard/static",
    emptyOutDir: true,
    sourcemap: false,
    ...(mode === "docs" ? { rollupOptions: { input: "docs.html" } } : {}),
  },
  server: {
    proxy: {
      "/api": "http://127.0.0.1:8765",
    },
  },
}));
