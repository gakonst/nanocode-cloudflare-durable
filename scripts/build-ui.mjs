import { build } from "esbuild";
import { readFile, rm } from "node:fs/promises";
import { basename } from "node:path";
import { render } from "ondine/compiler";
import { zyzz } from "zyzz/esbuild";

const ondine = {
  name: "ondine",
  setup(build) {
    build.onLoad({ filter: /\.mmd$/ }, async ({ path }) => ({
      contents: `export default ${JSON.stringify(render(await readFile(path, "utf8"), { idPrefix: basename(path, ".mmd") }).svg)};`,
      loader: "js",
    }));
  },
};

for (const path of ["public/assets", "public/app.js", "public/app.css", "public/how-it-works.js", "public/how-it-works.css", "public/chatgpt.js", "public/chatgpt.css", "public/claude.js", "public/claude.css", "public/zyzz-assets", "public/zyzz.css", "public/zyzz.css.map", "public/zyzz.js"]) await rm(path, { force: true, recursive: true });
await build({
  assetNames: "assets/[name]-[hash]",
  bundle: true,
  chunkNames: "assets/[name]-[hash]",
  entryNames: "[name]",
  entryPoints: { app: "ui/app.tsx", chatgpt: "ui/chatgpt.tsx", claude: "ui/claude.tsx", "how-it-works": "ui/how.tsx" },
  format: "esm",
  jsx: "automatic",
  loader: { ".wasm": "file", ".woff2": "file" },
  minify: true,
  outdir: "public",
  platform: "browser",
  plugins: [ondine, zyzz()],
  splitting: true,
  target: "es2022",
});
