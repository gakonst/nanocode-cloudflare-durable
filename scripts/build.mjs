import { spawnSync } from "node:child_process";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";

const result = spawnSync("cargo", ["build", "--locked", "--lib", "--target", "wasm32-unknown-unknown", "--profile", "wasm"], { stdio: "inherit" });
if (result.error) throw result.error;
if (result.status !== 0) process.exit(result.status ?? 1);
mkdirSync("dist", { recursive: true });
const bytes = readFileSync("target/wasm32-unknown-unknown/wasm/nanocode_worker.wasm");
let previous;
try { previous = readFileSync("dist/nanocode_worker.wasm"); } catch (error) { if (error.code !== "ENOENT") throw error; }
if (!previous?.equals(bytes)) writeFileSync("dist/nanocode_worker.wasm", bytes);
