import { spawnSync } from "node:child_process";
import { mkdirSync, readFileSync, writeFileSync, readdirSync, existsSync } from "node:fs";
import { createHash } from "node:crypto";
import { join } from "node:path";

const digest = createHash("sha256");
function include(path) {
  digest.update(path);
  digest.update(readFileSync(path));
}
function directory(path) {
  for (const file of readdirSync(path, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
    if (["target", "node_modules", ".git", "output", "dist"].includes(file.name)) continue;
    const child = join(path, file.name);
    if (file.isDirectory()) directory(child);
    else if (file.isFile()) include(child);
  }
}
for (const file of ["Cargo.toml", "Cargo.lock", "scripts/build.mjs", "../nanocodex/Cargo.toml", "../nanocodex/Cargo.lock"]) include(file);
for (const root of [".", "../nanocodex"]) for (const name of ["rust-toolchain", "rust-toolchain.toml", ".cargo/config", ".cargo/config.toml", "build.rs"]) if (existsSync(join(root, name))) include(join(root, name));
for (const file of readdirSync("src", { recursive: true }).filter(file => file.endsWith(".rs")).sort()) include(join("src", file));
directory("../nanocodex/crates");
const compiler = spawnSync("rustc", ["--version", "--verbose"], { encoding: "utf8" });
if (compiler.error) throw compiler.error;
if (compiler.status !== 0) process.exit(compiler.status ?? 1);
digest.update(compiler.stdout);
digest.update(JSON.stringify(Object.fromEntries(Object.entries(process.env).filter(([key]) => key.startsWith("CARGO_") || key.startsWith("RUST")))));
const fingerprint = digest.digest("hex");
const artifact = "target/wasm32-unknown-unknown/wasm/nanocode_worker.wasm";
let previousFingerprint;
try { previousFingerprint = readFileSync("output/wasm-build-fingerprint", "utf8"); } catch (error) { if (error.code !== "ENOENT") throw error; }
if (previousFingerprint !== fingerprint || !existsSync(artifact)) {
  const result = spawnSync("cargo", ["build", "--locked", "--lib", "--target", "wasm32-unknown-unknown", "--profile", "wasm"], { stdio: "inherit" });
  if (result.error) throw result.error;
  if (result.status !== 0) process.exit(result.status ?? 1);
  mkdirSync("output", { recursive: true });
  writeFileSync("output/wasm-build-fingerprint", fingerprint);
}
mkdirSync("dist", { recursive: true });
const bytes = readFileSync(artifact);
let previous;
try { previous = readFileSync("dist/nanocode_worker.wasm"); } catch (error) { if (error.code !== "ENOENT") throw error; }
if (!previous?.equals(bytes)) writeFileSync("dist/nanocode_worker.wasm", bytes);
