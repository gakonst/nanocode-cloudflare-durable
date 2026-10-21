import { copyFileSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { PUBLIC_HARNESSES } from "../worker/models.ts";

const require = createRequire(import.meta.url);
copyFileSync(require.resolve("nanocode-ts/client"), new URL("../public/client.js", import.meta.url));
writeFileSync(new URL("../public/models.js", import.meta.url), `export const harnesses = ${JSON.stringify(PUBLIC_HARNESSES)};\n`);
