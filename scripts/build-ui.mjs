import { build } from "esbuild";
import { copyFile, rm } from "node:fs/promises";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
await rm("public/assets", { force: true, recursive: true });
await copyFile(require.resolve("nanocode-ts/react/styles.css"), "public/chat.css");
await build({
  bundle: true,
  chunkNames: "assets/[name]-[hash]",
  entryNames: "[name]",
  entryPoints: { app: "ui/app.tsx", syntax: "ui/page.ts" },
  format: "esm",
  jsx: "automatic",
  minify: true,
  outdir: "public",
  platform: "browser",
  splitting: true,
  target: "es2022",
});
