import { build } from "esbuild";
import { rm } from "node:fs/promises";

await rm("public/syntax", { force: true, recursive: true });
await build({
  bundle: true,
  chunkNames: "syntax/[name]-[hash]",
  entryNames: "syntax",
  entryPoints: ["ui/syntax.ts"],
  format: "esm",
  minify: true,
  outdir: "public",
  platform: "browser",
  splitting: true,
  target: "es2022",
});
