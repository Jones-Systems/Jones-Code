import {
  ProviderInstanceId,
  type ServerConfig,
  type ServerConfigStreamEvent,
} from "@t3tools/contracts";
import * as Option from "effect/Option";
import { describe, expect, it } from "vite-plus/test";

import { applyServerConfigProjection, reuseUnchanged } from "./serverConfigProjection.ts";

describe("reuseUnchanged", () => {
  const provider = (checkedAt: string, skills: ReadonlyArray<string>) => ({
    instanceId: "claude",
    checkedAt,
    models: [{ slug: "opus", name: "Opus" }],
    skills: skills.map((name) => ({ name })),
  });

  it("takes the new value when keys or lengths change", () => {
    const previous = [provider("a", ["x"])];
    const next = reuseUnchanged(previous, [provider("a", ["x", "z"])]);
    expect(next[0]!.skills).toHaveLength(2);
    expect(next[0]!.models).toBe(previous[0]!.models);
    const dropped = reuseUnchanged({ a: 1, b: 2 }, { a: 1 });
    expect(dropped).toEqual({ a: 1 });
  });
});

describe("applyServerConfigProjection settingsUpdated", () => {
  it("keeps unchanged settings sections when one field changes", () => {
    const settings = {
      favorites: [{ provider: "claude", model: "opus" }],
      providerInstances: {
        claude: { driver: "claudeAgent", config: { launchArgs: "" } },
        codex: { driver: "codex" },
      },
    };
    const current = Option.some({
      config: { settings } as unknown as ServerConfig,
      latestEvent: {} as ServerConfigStreamEvent,
      source: "live" as const,
    });
    const next = applyServerConfigProjection(current, {
      type: "settingsUpdated",
      payload: {
        settings: {
          favorites: [{ provider: "claude", model: "opus" }],
          providerInstances: {
            claude: { driver: "claudeAgent", config: { launchArgs: "--verbose" } },
            codex: { driver: "codex" },
          },
        },
      },
    } as unknown as ServerConfigStreamEvent);
    const nextSettings = Option.getOrThrow(next).config.settings as unknown as typeof settings;
    expect(nextSettings.favorites).toBe(settings.favorites);
    expect(nextSettings.providerInstances.codex).toBe(settings.providerInstances.codex);
    expect(nextSettings.providerInstances.claude.config.launchArgs).toBe("--verbose");
  });
});

describe("applyServerConfigProjection provider order", () => {
  it.each(["settingsUpdated", "snapshot"] as const)(
    "preserves reordered providers and reuses unchanged values on %s",
    (type) => {
      const claude = ProviderInstanceId.make("claude");
      const codex = ProviderInstanceId.make("codex");
      const settings = {
        favorites: [{ provider: "claude", model: "opus" }],
        providerInstances: {
          claude: { driver: "claudeAgent", config: { launchArgs: "" } },
          codex: { driver: "codex" },
        },
      };
      const config = {
        environment: { capabilities: {} },
        settings,
      } as unknown as ServerConfig;
      const current = Option.some({
        config,
        latestEvent: {} as ServerConfigStreamEvent,
        source: "live" as const,
      });
      const reorderedSettings = {
        favorites: [{ provider: "claude", model: "opus" }],
        providerInstances: {
          codex: { driver: "codex" },
          claude: { driver: "claudeAgent", config: { launchArgs: "" } },
        },
      };
      const event = (type === "snapshot"
        ? { type, config: { ...config, settings: reorderedSettings } }
        : { type, payload: { settings: reorderedSettings } }) as unknown as ServerConfigStreamEvent;

      const next = applyServerConfigProjection(current, event);
      const nextSettings = Option.getOrThrow(next).config.settings;
      expect(Object.keys(nextSettings.providerInstances)).toEqual(["codex", "claude"]);
      expect(nextSettings.providerInstances).not.toBe(settings.providerInstances);
      expect(nextSettings.providerInstances[codex]).toBe(settings.providerInstances.codex);
      expect(nextSettings.providerInstances[claude]).toBe(settings.providerInstances.claude);
      expect(nextSettings.favorites).toBe(settings.favorites);
      expect(Option.getOrThrow(applyServerConfigProjection(next, event)).config.settings).toBe(
        nextSettings,
      );
    },
  );
});
