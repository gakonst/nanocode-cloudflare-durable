import { codeToTokens, isSupportedLanguage } from "@pierre/highlights";
import { themes } from "./theme.ts";

export function highlight(code: string, language: string) {
  return codeToTokens(code, {
    defaultColor: "light-dark()",
    lang: isSupportedLanguage(language) ? language : "text",
    themes,
  });
}
