// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

import { assert, it } from "@effect/vitest";

import {
  buildTriageContext,
  buildTriageLaunchPrompt,
  buildTriageSeedPrompt,
  TRIAGE_PLAYBOOK,
} from "./triagePrompt.ts";

it("emits Jones source and issue bindings while keeping the t3 compatibility command", () => {
  const seed = buildTriageSeedPrompt("/synthetic/triage/context.md");
  assert.include(seed, "Jones Code triage playbook");
  assert.include(seed, "https://github.com/Jones-Systems/Jones-Code");
  assert.include(seed, "https://github.com/Jones-Systems/Jones-Code/issues/new");
  assert.include(seed, "raw.githubusercontent.com/Jones-Systems/Jones-Code/main/");
  assert.include(seed, "`t3 triage`");
  assert.notInclude(seed, "pingdotgg/t3code");
  assert.include(buildTriageLaunchPrompt("/synthetic/prompt.md"), "Jones Code triage playbook");
});

it("keeps diagnostic inspection bounded and private uploads opt-in", () => {
  assert.include(TRIAGE_PLAYBOOK, "Never upload raw diagnostic files automatically");
  assert.include(TRIAGE_PLAYBOOK, "explicit permission for the exact paths and time range");
  assert.include(TRIAGE_PLAYBOOK, "Do not delete existing cache entries");
  assert.notInclude(TRIAGE_PLAYBOOK, "Read it freely");
});

it("stays byte-identical to .github/triage/PLAYBOOK.md", () => {
  // Older releases fetch the repository playbook.
  const canonicalPath = NodePath.join(
    import.meta.dirname,
    "../../../../.github/triage/PLAYBOOK.md",
  );
  assert.equal(TRIAGE_PLAYBOOK, NodeFS.readFileSync(canonicalPath, "utf8"));
});

it("launch prompt stays a single argv-safe line naming the prompt file", () => {
  // Windows .cmd shims cannot carry multiline argv.
  const launch = buildTriageLaunchPrompt(String.raw`C:\Users\a b\.t3\userdata\triage\x\prompt.md`);
  assert.notInclude(launch, "\n");
  assert.include(launch, String.raw`C:\Users\a b\.t3\userdata\triage\x\prompt.md`);
  assert.isBelow(launch.length, 1_000);
});

it("context file labels the CLI version as the CLI's, not the user's build", () => {
  const context = buildTriageContext({
    generatedAt: "2026-09-24T00:00:00.000Z",
    cliVersion: "0.0.42",
    os: "linux x64 (7.2.0)",
    nodeVersion: "v26.8.2",
    launchedAs: "npx t3 triage",
    server: "running (pid 42, http://127.0.0.1:3773, version 0.0.43-nightly.20260923.2173)",
    paths: {
      stateDir: "/home/u/.t3/userdata",
      dbPath: "/home/u/.t3/userdata/state.sqlite",
      settingsPath: "/home/u/.t3/userdata/settings.json",
      logsDir: "/home/u/.t3/userdata/logs",
      serviceLogPath: "/home/u/.t3/userdata/logs/boot-service.log",
      desktopBackendLogGlob: "/home/u/.t3/userdata/logs/server-child*.log*",
      serverTracePath: "/home/u/.t3/userdata/logs/server.trace.ndjson",
      providerEventLogPath: "/home/u/.t3/userdata/logs/provider/events.log",
      terminalLogsDir: "/home/u/.t3/userdata/logs/terminals",
      providerStatusCacheDir: "/home/u/.t3/caches",
      secretsDir: "/home/u/.t3/userdata/secrets",
      sourceCacheDir: "/home/u/.t3/source",
    },
  });
  assert.notInclude(context, "Installed version");
  assert.notInclude(context, "Release tag");
  assert.include(context, "- Triage CLI version: 0.0.42 (");
  assert.include(context, "version 0.0.43-nightly.20260923.2173");
  assert.include(context, "# Jones Code triage context");
  assert.include(context, "- Repo: https://github.com/Jones-Systems/Jones-Code");
});
