import { readFileSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";

export function credentials(path) {
  let document;
  if (path) {
    document = JSON.parse(readFileSync(path, "utf8"));
  } else {
    const directory = process.env.CLAUDE_SECURESTORAGE_CONFIG_DIR ?? process.env.CLAUDE_CONFIG_DIR;
    const suffix = directory ? `-${createHash("sha256").update(directory.normalize("NFC")).digest("hex").slice(0, 8)}` : "";
    const result = spawnSync("security", ["find-generic-password", "-w", "-s", `Claude Code-credentials${suffix}`], { encoding: "utf8" });
    if (result.status !== 0) throw new Error("Pass the path to oauth.json, or sign in to Claude Code on this Mac");
    document = JSON.parse(result.stdout);
  }
  const token = document.anthropic ?? document.claudeAiOauth ?? document;
  const credential = { access: token.access ?? token.accessToken, refresh: token.refresh ?? token.refreshToken, expires: token.expires ?? token.expiresAt };
  if (typeof credential.access !== "string" || typeof credential.refresh !== "string" || !Number.isSafeInteger(credential.expires)) throw new Error("No Claude subscription OAuth credentials found in the document");
  return credential;
}
