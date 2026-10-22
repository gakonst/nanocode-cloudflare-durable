import { highlight } from "nanocode-ts/highlight";

for (const block of document.querySelectorAll<HTMLElement>("pre[data-language] > code")) {
  const tokens = highlight(block.textContent ?? "", block.parentElement!.dataset.language ?? "text");
  block.replaceChildren(...tokens.flatMap((line, row) => {
    const nodes: Node[] = row ? [document.createTextNode("\n")] : [];
    for (const token of line) {
      const span = document.createElement("span");
      span.textContent = token.content;
      if (token.color) span.style.color = token.color;
      if (token.italic) span.style.fontStyle = "italic";
      nodes.push(span);
    }
    return nodes;
  }));
}
