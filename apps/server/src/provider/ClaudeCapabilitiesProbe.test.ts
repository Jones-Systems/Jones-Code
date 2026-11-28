// @effect-diagnostics nodeBuiltinImport:off - cleanup uses Node's retrying rm, which the FileSystem service does not expose.
import * as NodeServices from "@effect/platform-node/NodeServices";
import { ClaudeSettings, type ServerProvider } from "@t3tools/contracts";
import { vi } from "vite-plus/test";
import * as ClaudeSdk from "@anthropic-ai/claude-agent-sdk";
import { assert, it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";

import {
  buildClaudeCapabilitiesProbeQueryOptions,
  probeClaudeCapabilities,
  probeClaudeWorkspaceSnapshot,
  readClaudeQueryUsage,
  spawnClaudeProbeThroughEnv,
} from "./ClaudeProvider.ts";

vi.mock("@anthropic-ai/claude-agent-sdk", { spy: true });

const hostPath = process.env.PATH;

const decodeClaudeSettings = Schema.decodeEffect(ClaudeSettings);

it("drops strict MCP mode, and only that, next to an administrator's managed-mcp.json", () => {
  const options = buildClaudeCapabilitiesProbeQueryOptions({
    executablePath: "/usr/bin/claude",
    abortController: new AbortController(),
    environment: { HOME: "/home/user" },
    cwd: undefined,
    managedMcpConfig: true,
    spawnVia: undefined,
  });

  // Claude exits before initializing when handed --strict-mcp-config here.
  assert.equal(options.strictMcpConfig, undefined);
  assert.deepEqual(options.mcpServers, {});
  assert.equal(options.env?.ENABLE_CLAUDEAI_MCP_SERVERS, "false");
});

it("isolates Claude Auto from ambient settings, tools, and MCP", () => {
  const options = buildClaudeCapabilitiesProbeQueryOptions({
    executablePath: "/usr/bin/claude",
    abortController: new AbortController(),
    environment: {
      HOME: "/home/user",
      CLAUDE_CONFIG_DIR: "/synthetic/account",
      ENABLE_CLAUDEAI_MCP_SERVERS: "true",
      CLAUDE_CODE_PLUGIN_DIRS: "/synthetic/inline-plugin",
    },
    cwd: "/synthetic/project",
    managedMcpConfig: false,
    spawnVia: undefined,
    autoRouting: true,
  });
  assert.equal(options.strictMcpConfig, true);
  assert.deepEqual(options.settingSources, []);
  assert.deepEqual(options.tools, []);
  assert.deepEqual(options.plugins, []);
  assert.deepEqual(options.mcpServers, {});
  assert.deepEqual(options.settings, {
    disableAllHooks: true,
    env: { CLAUDE_CODE_PLUGIN_DIRS: "" },
  });
  assert.equal(options.env?.ENABLE_CLAUDEAI_MCP_SERVERS, "false");
  assert.equal(options.env?.CLAUDE_CODE_PLUGIN_DIRS, "");
  assert.equal(options.env?.CLAUDE_CONFIG_DIR, "/synthetic/account");
});

it.effect("refuses the Claude Auto probe before SDK startup when MCP is managed", () =>
  Effect.gen(function* () {
    const settings = yield* decodeClaudeSettings({ binaryPath: "/synthetic/claude" });
    const result = yield* probeClaudeCapabilities(
      settings,
      {},
      "/synthetic/project",
      true,
      true,
    ).pipe(Effect.result);
    assert.equal(Result.isFailure(result), true);
    if (Result.isFailure(result)) {
      assert.equal(result.failure.message, "Claude capability probe managedMcp failed.");
    }
  }).pipe(
    Effect.provideService(HostProcess.Platform, "linux"),
    Effect.provide(
      Layer.mergeAll(
        FileSystem.layerNoop({
          exists: (file) => Effect.succeed(file.endsWith("managed-mcp.json")),
        }),
        Path.layer,
      ),
    ),
  ),
);

it.effect("bounds the Claude Auto usage control when an older CLI does not reply", () =>
  Effect.gen(function* () {
    const usage = yield* readClaudeQueryUsage({
      usage_EXPERIMENTAL_MAY_CHANGE_DO_NOT_RELY_ON_THIS_API_YET: () => new Promise(() => {}),
    }).pipe(Effect.forkChild);
    yield* TestClock.adjust("4 seconds");
    assert.isUndefined(yield* Fiber.join(usage));
  }),
);

// A shell trampoline would not do: dash drops the exported function, and the launcher exits 127.
it.live.skipIf(HostProcess.Platform.defaultValue() !== "linux")(
  "starts a Bash launcher that calls an exported function in the probe's workspace",
  () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const workspace = yield* fileSystem.realPath(yield* fileSystem.makeTempDirectoryScoped());
      yield* fileSystem.writeFileString(`${workspace}/claude`, '#!/bin/bash\nt3_launch "$@"\n');
      yield* fileSystem.chmod(`${workspace}/claude`, 0o755);
      const child = spawnClaudeProbeThroughEnv({
        command: "claude",
        args: ["--probe"],
        cwd: workspace,
        env: { PATH: `.:${hostPath}`, "BASH_FUNC_t3_launch%%": '() { echo "$PWD $@"; }' },
        // @effect-diagnostics-next-line abortControllerInEffect:off - The launcher exits on its own.
        signal: new AbortController().signal,
      });
      assert.deepEqual(child.spawnargs.slice(0, 3), ["/usr/bin/env", "-C", workspace]);
      const output = yield* Effect.promise(() => child.stdout.setEncoding("utf8").toArray());
      assert.equal(output.join(""), `${workspace} --probe\n`);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("reports a deleted worktree's scan as finished, not pending", () =>
  Effect.gen(function* () {
    const snapshot = yield* probeClaudeWorkspaceSnapshot(
      yield* decodeClaudeSettings({}),
      { skills: [], slashCommands: [] } as unknown as ServerProvider,
      "/nonexistent/t3-deleted-worktree",
      { HOME: "/nonexistent/t3-home" },
    );
    assert.strictEqual(snapshot.slashCommandsPending, false);
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("keeps the probe subprocess alive while gateway model discovery can finish", () =>
  Effect.gen(function* () {
    let abortSignal: AbortSignal | undefined;
    const query = vi.spyOn(ClaudeSdk, "query").mockImplementation(({ options }) => {
      abortSignal = options?.abortController?.signal;
      return {
        initializationResult: async () => ({
          account: { email: "dev@example.com", subscriptionType: "pro", tokenSource: "oauth" },
          commands: [{ name: "review", description: "Review changes", argumentHint: "[path]" }],
          models: [{ value: "gateway/claude-relay", displayName: "Relay", description: "" }],
        }),
        usage_EXPERIMENTAL_MAY_CHANGE_DO_NOT_RELY_ON_THIS_API_YET: () =>
          new Promise((_, reject) => reject(new Error("usage unavailable"))),
      } as ReturnType<typeof ClaudeSdk.query>;
    });
    yield* Effect.addFinalizer(() => Effect.sync(() => query.mockRestore()));
    const fileSystem = yield* FileSystem.FileSystem;
    const configDir = yield* fileSystem.makeTempDirectoryScoped();
    // Claude Code also reads these from settings.json, which is where gateway users set them.
    yield* fileSystem.writeFileString(
      `${configDir}/settings.json`,
      JSON.stringify({
        env: {
          ANTHROPIC_BASE_URL: "https://gateway.example",
          CLAUDE_CODE_ENABLE_GATEWAY_MODEL_DISCOVERY: "1",
        },
      }),
    );
    const capabilities = yield* probeClaudeCapabilities(
      yield* decodeClaudeSettings({ binaryPath: "claude" }),
      { PATH: (yield* HostProcess.Environment).PATH, CLAUDE_CONFIG_DIR: configDir },
    );
    assert.deepEqual(
      capabilities.models?.map((model) => model.value),
      ["gateway/claude-relay"],
    );
    assert.equal(abortSignal?.aborted, false);
    yield* TestClock.adjust("3 seconds");
    assert.equal(abortSignal?.aborted, true);
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
