import { performance } from "node:perf_hooks";

const size = Number(process.argv[2] ?? 65536);
const chunks = Number(process.argv[3] ?? 256);
if (!Number.isSafeInteger(size) || size <= 0 || !Number.isSafeInteger(chunks) || chunks <= 0) throw new Error("Pass positive chunk size and count");
const source = new Uint8Array(size);
for (let i = 0; i < size; i++) source[i] = i % 256;
const destination = new Uint8Array(size);
let checksum = 0;
function measure(name, transfer) {
  const start = performance.now();
  for (let i = 0; i < chunks; i++) checksum += transfer();
  return { name, milliseconds: performance.now() - start, bytes: size * chunks };
}
const results = [
  measure("JSON byte arrays", () => { const decoded = JSON.parse(JSON.stringify(Array.from(source))); destination.set(decoded); return destination[size - 1]; }),
  measure("binary WASM memory copy", () => { destination.set(source); return destination[size - 1]; }),
];
console.log(JSON.stringify({ node: process.version, chunk_bytes: size, chunks, results, checksum }, null, 2));
