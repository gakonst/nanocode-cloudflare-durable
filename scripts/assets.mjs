import { copyFileSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { PUBLIC_HARNESSES } from "../worker/models.ts";

const require = createRequire(import.meta.url);
for (const [specifier, file] of [["nanocode-ts/client", "client.js"], ["nanocode-ts/ui", "ui.js"], ["nanocode-ts/ui.css", "ui.css"]]) copyFileSync(require.resolve(specifier), new URL(`../public/${file}`, import.meta.url));
writeFileSync(new URL("../public/models.js", import.meta.url), `export const harnesses = ${JSON.stringify(PUBLIC_HARNESSES)};\n`);
