import { readFileSync } from "node:fs";
import { spawnSync } from "node:child_process";

export function credentials(path) {
  let document;
  if (path) {
    document = JSON.parse(readFileSync(path, "utf8"));
  } else {
    const result = spawnSync("security", ["find-generic-password", "-w", "-s", "Claude Code-credentials"], { encoding: "utf8" });
    if (result.status !== 0) throw new Error("Pass the path to oauth.json, or sign in to Claude Code on this Mac");
    document = JSON.parse(result.stdout);
  }
  const token = document.anthropic ?? document.claudeAiOauth ?? document;
  const credential = { access: token.access ?? token.accessToken, refresh: token.refresh ?? token.refreshToken, expires: token.expires ?? token.expiresAt };
  if (typeof credential.access !== "string" || typeof credential.refresh !== "string" || !Number.isSafeInteger(credential.expires)) throw new Error("No Claude subscription OAuth credentials found in the document");
  return credential;
}
