import { createConfig } from "nanocode-ts/client";
import { cloudflare } from "nanocode-ts/cloudflare/transport";
import { Keyring } from "nanocode-ts/machines";
import { useState, useSyncExternalStore } from "react";
import { PUBLIC_HARNESSES } from "../../worker/models.ts";

export type Model = (typeof PUBLIC_HARNESSES)[number]["models"][number];

type Saved = { picked?: string; bound: Record<string, string> };

const TOKEN = "nanocode-cloudflare-durable:token";

export const highlight = () => import("nanocode-ts/highlight").then(module => module.highlight);

export function createClone(key: string, harness: "claude" | "codex") {
  const models: readonly Model[] = PUBLIC_HARNESSES.find(entry => entry.id === harness)?.models ?? [];
  const listeners = new Set<() => void>();
  const storageKey = key + ":models";
  let saved: Saved = { bound: {} };
  let version = 0;

  try {
    saved = { ...saved, ...JSON.parse(localStorage.getItem(storageKey) ?? "{}") };
  } catch {}

  const persist = () => {
    const ids = new Set(config.getState().conversations.map(item => item.id));
    saved.bound = Object.fromEntries(Object.entries(saved.bound).filter(([id]) => ids.has(id)));
    localStorage.setItem(storageKey, JSON.stringify(saved));
    version++;
    for (const listener of listeners) listener();
  };

  const headers = (): Record<string, string> => {
    const token = localStorage.getItem(TOKEN);
    return { ...Keyring.visitorHeaders(), ...(token ? { authorization: "Bearer " + token } : {}) };
  };

  const current = (): Model => {
    const conversation = config.getState().conversation;
    const id = (conversation && saved.bound[conversation]) || saved.picked;
    return models.find(model => model.id === id) ?? models[0]!;
  };

  const transport = cloudflare({
    headers,
    model: () => {
      const model = current();
      const conversation = config.getState().conversation;

      if (conversation && transport.session().id && !saved.bound[conversation]) {
        saved.bound[conversation] = model.id;
        persist();
      }

      return { model: model.id, provider: harness, thinking: (model.efforts as readonly string[]).includes("low") ? "low" : model.effort };
    },
  });

  const config = createConfig({ history: { key }, transport });

  const choose = (id: string) => {
    const conversation = config.getState().conversation;
    saved.picked = id;
    if (conversation && saved.bound[conversation] && saved.bound[conversation] !== id) config.create();
    persist();
  };

  const subscribe = (listener: () => void) => {
    listeners.add(listener);
    const off = config.subscribe(listener);
    return () => {
      listeners.delete(listener);
      off();
    };
  };

  const useModel = () => {
    useSyncExternalStore(subscribe, () => version + ":" + config.getState().conversation);
    const conversation = config.getState().conversation;
    return { bound: Boolean(conversation && saved.bound[conversation]), choose, model: current(), models };
  };

  return { config, transport, useModel };
}

