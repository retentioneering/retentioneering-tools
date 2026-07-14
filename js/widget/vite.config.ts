import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path from "path";

export default defineConfig({
  plugins: [react()],
  define: {
    "process.env.NODE_ENV": JSON.stringify("production"),
  },
  build: {
    lib: {
      entry: path.resolve(__dirname, "src/main.tsx"),
      formats: ["es", "iife"],
      name: "RetentioneeringWidget",
      fileName: (fmt) => fmt === "iife" ? "widget-static.js" : "widget.js",
    },
    outDir: "../../src/retentioneering/static",
    emptyOutDir: false,
    rollupOptions: {
      output: {
        assetFileNames: "widget.css",
        inlineDynamicImports: true,
      },
    },
  },
});
