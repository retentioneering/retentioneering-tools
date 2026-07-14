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
      entry: path.resolve(__dirname, "src/index.ts"),
      name: "RetentioneeringVizCore",
      formats: ["es"],
      fileName: () => "viz-core.js",
    },
    outDir: "dist",
    rollupOptions: {
      // React is external so fe/widget can share one instance
      external: ["react", "react-dom", "react/jsx-runtime", "mobx", "mobx-react-lite"],
      output: {
        globals: {
          react: "React",
          "react-dom": "ReactDOM",
        },
        assetFileNames: "viz-core.css",
      },
    },
  },
});
