import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path from "path";

export default defineConfig({
  plugins: [react()],
  define: {
    "process.env.NODE_ENV": JSON.stringify("production"),
    // No cloud backend ships with this open-source distribution — these are
    // empty by default. The cloud icon itself only renders when the Python
    // side also opts in via RETENTIONEERING_CLOUD_ENABLED.
    __SUPABASE_URL__:      JSON.stringify(process.env.RETENTIONEERING_CLOUD_SUPABASE_URL      ?? ""),
    __SUPABASE_ANON_KEY__: JSON.stringify(process.env.RETENTIONEERING_CLOUD_SUPABASE_ANON_KEY ?? ""),
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
