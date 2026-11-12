import * as NodeOS from "node:os";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as HostProcess from "@t3tools/shared/HostProcess";

import {
  makeClaudeCapabilitiesCacheKey,
  makeClaudeContinuationGroupKey,
  makeClaudeEnvironment,
  mergeClaudeProcessEnvironment,
  resolveClaudeHomePath,
  resolveClaudeUserHome,
  usesClaudeHostConfig,
} from "./ClaudeHome.ts";

it.layer(NodeServices.layer)("ClaudeHome", (it) => {
  describe("Claude home resolution", () => {
    it.effect("uses provider HOME for default config, expansion and account identities", () =>
      Effect.gen(function* () {
        const path = yield* Path.Path;
        const environment = { HOME: "/synthetic/provider-home" };
        const original = {
          HOME: process.env.HOME,
          CLAUDE_CONFIG_DIR: process.env.CLAUDE_CONFIG_DIR,
        };
        const config = { binaryPath: "claude", homePath: "" };
        const resolved = path.resolve(environment.HOME, ".claude");
        expect(yield* resolveClaudeUserHome(environment)).toBe(environment.HOME);
        expect(yield* resolveClaudeHomePath(config, environment)).toBe(resolved);
        expect(yield* makeClaudeContinuationGroupKey(config, environment)).toBe(
          `claude:home:${resolved}`,
        );
        expect(yield* makeClaudeCapabilitiesCacheKey(config, "/repo", environment)).toBe(
          `claude\0${resolved}\0/repo`,
        );
        expect(yield* makeClaudeCapabilitiesCacheKey(config, "/repo", { HOME: "/other" })).not.toBe(
          yield* makeClaudeCapabilitiesCacheKey(config, "/repo", environment),
        );
        for (const inherited of [undefined, "", "/inherited", "~/literal"]) {
          const baseEnv = { ...environment, CLAUDE_CONFIG_DIR: inherited };
          expect(yield* makeClaudeEnvironment(config, baseEnv)).toBe(baseEnv);
          expect(yield* resolveClaudeHomePath(config, baseEnv)).toBe(
            inherited ? path.resolve(inherited) : resolved,
          );
          const explicit = yield* makeClaudeEnvironment({ homePath: "~/.claude-work" }, baseEnv);
          expect(explicit.HOME).toBe(environment.HOME);
          expect(explicit.CLAUDE_CONFIG_DIR).toBe(path.resolve(environment.HOME, ".claude-work"));
        }
        const absent = yield* makeClaudeEnvironment(config, environment);
        expect(Object.hasOwn(absent, "CLAUDE_CONFIG_DIR")).toBe(false);
        expect({
          HOME: process.env.HOME,
          CLAUDE_CONFIG_DIR: process.env.CLAUDE_CONFIG_DIR,
        }).toEqual(original);
      }).pipe(Effect.provideService(HostProcess.HomeDirectory, "/synthetic/server-home")),
    );

    it.effect("isolates SDK calls for HOME changes and absent versus explicit config", () =>
      Effect.gen(function* () {
        expect(yield* usesClaudeHostConfig({ HOME: "/server" })).toBe(true);
        expect(yield* usesClaudeHostConfig({})).toBe(true);
        expect(yield* usesClaudeHostConfig({ HOME: "/provider" })).toBe(false);
        expect(yield* usesClaudeHostConfig({ HOME: "/server", CLAUDE_CONFIG_DIR: "" })).toBe(false);
        expect(
          yield* usesClaudeHostConfig({ HOME: "/server", CLAUDE_CONFIG_DIR: "/server/.claude" }),
        ).toBe(false);
      }).pipe(
        Effect.provideService(HostProcess.HomeDirectory, "/server"),
        Effect.provideService(HostProcess.Environment, { HOME: "/server" }),
      ),
    );

    it.effect("resolves configured Claude HOME and stamps continuation/cache keys with it", () =>
      Effect.gen(function* () {
        const path = yield* Path.Path;
        const homePath = "~/.claude-work";
        const resolved = path.resolve(NodeOS.homedir(), ".claude-work");

        expect(yield* resolveClaudeHomePath({ homePath })).toBe(resolved);
        expect((yield* makeClaudeEnvironment({ homePath })).CLAUDE_CONFIG_DIR).toBe(resolved);
        expect(yield* makeClaudeContinuationGroupKey({ homePath })).toBe(`claude:home:${resolved}`);
        expect(yield* makeClaudeCapabilitiesCacheKey({ binaryPath: "claude", homePath })).toBe(
          `claude\0${resolved}\0`,
        );
      }),
    );

    it.effect("uses inherited CLAUDE_CONFIG_DIR when homePath is empty", () =>
      Effect.gen(function* () {
        const path = yield* Path.Path;
        const inherited = path.resolve("/tmp/claude-inherited");
        const environment = { CLAUDE_CONFIG_DIR: inherited };

        expect(yield* resolveClaudeHomePath({ homePath: "" }, environment)).toBe(inherited);
        expect(yield* makeClaudeContinuationGroupKey({ homePath: "" }, environment)).toBe(
          `claude:home:${inherited}`,
        );

        const explicit = path.resolve(NodeOS.homedir(), ".claude-work");
        expect(yield* resolveClaudeHomePath({ homePath: "~/.claude-work" }, environment)).toBe(
          explicit,
        );
      }),
    );

    it.effect("separates capability probes by cwd", () =>
      Effect.gen(function* () {
        const config = { binaryPath: "claude", homePath: "" };
        const first = yield* makeClaudeCapabilitiesCacheKey(config, "/repo-a");
        const second = yield* makeClaudeCapabilitiesCacheKey(config, "/repo-b");
        expect(first).not.toBe(second);
      }),
    );
  });

  it.effect(
    "drops the launching Claude session and Electron's run-as-Node flag, and labels processes t3-code unless overridden",
    () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const homePath = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-claude-home-" });
        const host = {
          CLAUDE_CODE_ENTRYPOINT: "claude-desktop",
          CLAUDECODE: "1",
          CLAUDE_CODE_SESSION_ID: "host-session",
          CLAUDE_CODE_MESSAGING_SOCKET: "/tmp/host-claude.sock",
          CLAUDE_EFFORT: "high",
          ELECTRON_RUN_AS_NODE: "1",
          KEEP: "1",
        };
        const instance = [
          { name: "CLAUDE_CODE_ENTRYPOINT", value: "instance", sensitive: false },
          { name: "CLAUDE_EFFORT", value: "low", sensitive: false },
        ];

        expect(yield* mergeClaudeProcessEnvironment({ homePath }, undefined, host)).toEqual({
          CLAUDE_CODE_ENTRYPOINT: "t3-code",
          KEEP: "1",
        });
        expect(yield* mergeClaudeProcessEnvironment({ homePath }, instance, host)).toEqual({
          CLAUDE_CODE_ENTRYPOINT: "instance",
          CLAUDE_EFFORT: "low",
          KEEP: "1",
        });

        yield* fileSystem.writeFileString(
          path.join(homePath, "settings.json"),
          JSON.stringify({ env: { CLAUDE_CODE_ENTRYPOINT: "custom" } }),
        );
        expect(
          (yield* mergeClaudeProcessEnvironment({ homePath }, instance, host))
            .CLAUDE_CODE_ENTRYPOINT,
        ).toBe("custom");
      }),
  );
});
