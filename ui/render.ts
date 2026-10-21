import { fences } from "./fences.ts";

const cache = new Map<string, ReturnType<typeof import("./highlight.ts").highlight>>();

function code(source: string, language: string) {
  const pre = document.createElement("pre");
  pre.className = "code";
  const content = document.createElement("code");
  content.textContent = source;
  pre.append(content);
  paint(content, source, language);
  return pre;
}

function paint(content: HTMLElement, source: string, language: string) {
  void import("./highlight.ts").then(({ highlight }) => {
    if (!content.isConnected) return;
    const key = `${language}\0${source}`;
    const result = cache.get(key) ?? highlight(source, language);
    cache.delete(key);
    cache.set(key, result);
    if (cache.size > 64) cache.delete(cache.keys().next().value!);
    content.replaceChildren(...result.tokens.flatMap((line, row) => {
      const nodes: Node[] = row ? [document.createTextNode("\n")] : [];
      for (const token of line) {
        const span = document.createElement("span");
        span.textContent = token.content;
        const color = token.htmlStyle?.color ?? token.color;
        if (color) span.style.color = color;
        if (token.htmlStyle?.["font-style"] === "italic" || token.fontStyle && token.fontStyle & 1) span.style.fontStyle = "italic";
        nodes.push(span);
      }
      return nodes;
    }));
  }).catch(() => {});
}

export function reply(text: string) {
  const container = document.createElement("div");
  for (const part of fences(text)) container.append(part.kind === "code" ? code(part.text, part.language) : document.createTextNode(part.text));
  return container;
}

for (const block of document.querySelectorAll<HTMLElement>("pre[data-language] > code"))
  paint(block, block.textContent ?? "", block.parentElement!.dataset.language ?? "text");
