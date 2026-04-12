import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  // If deploying to GitHub Pages at username.github.io/repo-name,
  // set base to "/repo-name/". For a custom domain, leave as "/".
  base: "/selected-cinema-screener/",
});
