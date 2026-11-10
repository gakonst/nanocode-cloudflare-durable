import type { Contracts } from "nanocode-ts";

type ModelProvider = Contracts.ModelProvider;
import { Plans } from "nanocode-ts/client";
import { models } from "nanocode-ts/models";

type Public = { id: ModelProvider; label: string; models: { id: string; label: string; detail: string }[] };

const PUBLIC: Public[] = [
  { id: "claude", label: "Claude", models: [{ id: "claude-haiku-5-5", label: "Haiku 5.5", detail: "Fastest for quick answers" }, { id: "claude-sonnet-5-5", label: "Sonnet 5.5", detail: "Smart, efficient model for everyday use" }] },
  { id: "codex", label: "Codex", models: [{ id: "gpt-6-luna", label: "Luna", detail: "Fast for everyday questions" }, { id: "gpt-6.1-sol", label: "Sol", detail: "Thinks longer for harder problems" }, { id: "glm-5.3", label: "GLM-5.3", detail: "Open-weights model on Workers AI" }] },
];

function harnesses(list: Public[]) {
  return list.map(harness => ({
    id: harness.id,
    label: harness.label,
    models: harness.models.map(({ id, label, detail }) => {
      const entry = models.harnesses.find(candidate => candidate.id === harness.id)?.models.find(candidate => candidate.id === id);
      if (!entry) throw new Error(`nanocode-ts's ${harness.id} catalog has no ${id}`);
      return { id, label, detail, name: entry.name, efforts: entry.efforts, effort: entry.effort };
    }),
  }));
}

function catalog(list: { id: ModelProvider; models: { id: string; efforts: readonly string[] }[] }[]): Record<string, { provider: ModelProvider; efforts: readonly string[] }> {
  return Object.fromEntries(list.flatMap(harness => harness.models.map(model => [model.id, { provider: harness.id, efforts: model.efforts }])));
}

export const PUBLIC_HARNESSES = harnesses(PUBLIC);

export const PUBLIC_MODELS = catalog(PUBLIC_HARNESSES);

export const CLAUDE_MODELS = new Set(models.harnesses.find(harness => harness.id === "claude")?.models.map(model => model.id));

export const PLAN_HARNESSES = PUBLIC_HARNESSES.map(harness => ({
  ...harness,
  models: (models.harnesses.find(candidate => candidate.id === harness.id)?.models ?? [])
    .filter(model => Plans.serves(harness.id, model.id))
    .map(model => ({ id: model.id, label: model.name, name: model.name, efforts: model.efforts, effort: model.effort })),
}));

export const PLAN_MODELS = catalog(PLAN_HARNESSES);
