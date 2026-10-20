import { spawnSync } from "node:child_process";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { gzipSync } from "node:zlib";

const target = process.env.CARGO_TARGET_DIR ?? "target/size";
const build = spawnSync("cargo", ["build", "--locked", "--lib", "--target", "wasm32-unknown-unknown", "--profile", "wasm"], { stdio: "inherit", env: { ...process.env, CARGO_TARGET_DIR: target, CARGO_PROFILE_WASM_STRIP: "none" } });
if (build.error) throw build.error;
if (build.status !== 0) process.exit(build.status ?? 1);
const named = `${target}/wasm32-unknown-unknown/wasm/nanocode_worker.wasm`;
const twiggy = spawnSync("twiggy", ["top", "-n", "1000000", "-f", "json", named], { encoding: "utf8", maxBuffer: 1 << 30 });
if (twiggy.error) throw new Error(`twiggy is required: cargo install twiggy (${twiggy.error.message})`);
if (twiggy.status !== 0) throw new Error(twiggy.stderr);
const strip = name => name.replace(/\[[0-9a-f]+\]/g, "");
const crates = new Map();
const functions = [];
let code = 0;
for (const { name, shallow_size: size } of JSON.parse(twiggy.stdout)) {
  const crate = name.match(/([A-Za-z_][A-Za-z0-9_]*)\[[0-9a-f]+\]/)?.[1];
  if (!crate) continue;
  code += size;
  crates.set(crate, (crates.get(crate) ?? 0) + size);
  functions.push({ name: strip(name), bytes: size });
}
const dist = readFileSync("dist/nanocode_worker.wasm");
const report = {
  dist: { bytes: dist.length, gzip_bytes: gzipSync(dist, { level: 9 }).length },
  attributed_code_bytes: code,
  crates: Object.fromEntries([...crates].sort((a, b) => b[1] - a[1])),
  functions: functions.sort((a, b) => b.bytes - a.bytes).slice(0, 200),
};
mkdirSync("output", { recursive: true });
writeFileSync("output/wasm-size.json", JSON.stringify(report, null, 2));
console.log(JSON.stringify({ dist: report.dist, attributed_code_bytes: code, crates: Object.fromEntries(Object.entries(report.crates).slice(0, 30)) }, null, 2));
