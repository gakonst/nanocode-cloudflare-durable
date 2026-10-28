import type { Contracts } from "nanocode-ts";

type ModelProvider = Contracts.ModelProvider;
import { Plans } from "nanocode-ts/client";
import { models } from "nanocode-ts/models";

type Public = { id: ModelProvider; label: string; models: { id: string; label: string }[] };

const PUBLIC: Public[] = [
  { id: "claude", label: "Claude", models: [{ id: "claude-haiku-5-5", label: "Haiku 5.5" }, { id: "claude-sonnet-5-5", label: "Sonnet 5.5" }] },
  { id: "codex", label: "Codex", models: [{ id: "gpt-6-luna", label: "Luna" }, { id: "gpt-6.1-sol", label: "Sol" }, { id: "glm-5.3", label: "GLM-5.3" }] },
];

export const PUBLIC_HARNESSES = PUBLIC.map(harness => ({
  id: harness.id,
  label: harness.label,
  models: harness.models.map(({ id, label }) => {
    const entry = models.harnesses.find(candidate => candidate.id === harness.id)?.models.find(candidate => candidate.id === id);
    if (!entry) throw new Error(`nanocode-ts's ${harness.id} catalog has no ${id}`);
    return { id, label, name: entry.name, efforts: entry.efforts, effort: entry.effort };
  }),
}));

export const PUBLIC_MODELS: Record<string, { provider: ModelProvider; efforts: readonly string[] }> = Object.fromEntries(PUBLIC_HARNESSES.flatMap(harness => harness.models.map(model => [model.id, { provider: harness.id, efforts: model.efforts }])));

export const PLAN_HARNESSES = PUBLIC_HARNESSES.map(harness => ({
  ...harness,
  models: (models.harnesses.find(candidate => candidate.id === harness.id)?.models ?? [])
    .filter(model => Plans.serves(harness.id, model.id))
    .map(model => ({ id: model.id, label: model.name, name: model.name, efforts: model.efforts, effort: model.effort })),
}));

export const PLAN_MODELS: Record<string, { provider: ModelProvider; efforts: readonly string[] }> = Object.fromEntries(PLAN_HARNESSES.flatMap(harness => harness.models.map(model => [model.id, { provider: harness.id, efforts: model.efforts }])));
