export function fences(text: string): ({ kind: "text"; text: string } | { kind: "code"; text: string; language: string })[] {
  const parts: ReturnType<typeof fences> = [];
  const openings = /^ {0,3}(`{3,}|~{3,})([^\r\n]*)\r?\n/gm;
  let offset = 0;
  for (const opening of text.matchAll(openings)) {
    if (opening.index < offset) continue;
    const marker = opening[1]!;
    const start = opening.index + opening[0].length;
    const closing = new RegExp(`^ {0,3}${marker[0]}{${marker.length},}[ \\t]*(?:\\r?\\n|$)`, "gm");
    closing.lastIndex = start;
    const end = closing.exec(text);
    if (opening.index > offset) parts.push({ kind: "text", text: text.slice(offset, opening.index) });
    parts.push({ kind: "code", language: opening[2]!.trim().split(/\s/)[0] || "text", text: text.slice(start, end?.index ?? text.length) });
    offset = end ? end.index + end[0].length : text.length;
  }
  if (offset < text.length) parts.push({ kind: "text", text: text.slice(offset) });
  return parts;
}
