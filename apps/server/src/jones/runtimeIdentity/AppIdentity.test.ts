import * as NodeModule from "node:module";
import * as NodeVM from "node:vm";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { ProviderInstanceId } from "@t3tools/contracts";
import { APP_IDENTITY, readAppIdentity } from "./AppIdentity.ts";
import { buildRuntimeInstructions } from "../../provider/RuntimeInstructions.ts";
import { buildCodexAdditionalContext } from "../../provider/CodexDeveloperInstructions.ts";
import { T3_CODE_BROWSER_TOOL_INSTRUCTIONS } from "../../provider/T3OrchestrationInstructions.ts";
import { buildCodexTurnStartParams } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import { PI_T3_MCP_EXTENSION_SOURCE } from "../../orchestration-v2/Adapters/piT3McpExtensionSource.ts";

const source = {
  repository: "Jones-Systems/Jones-Code",
  sha: "a".repeat(40),
  tree: "b".repeat(40),
};

describe("Jones agent app identity", () => {
  it.each(["0.0.45", "0.0.45-preview.20261008.37762212730.1", "0.0.46-nightly.20261008.2801"])(
    "identifies Jones independently of version naming: %s",
    (version) => {
      expect(readAppIdentity({ version })).toEqual({
        productId: "jones-code",
        productName: "Jones Code",
        version,
        source: null,
      });
    },
  );

  it("uses only validated bundled provenance, excluding unrelated manifest fields", () => {
    expect(
      readAppIdentity({ version: "1.0.0", jonesSource: { ...source, private: "omit" } }).source,
    ).toEqual(source);
    for (const jonesSource of [
      undefined,
      null,
      {},
      { ...source, repository: "pingdotgg/t3code" },
      { ...source, sha: "a".repeat(39) },
      { ...source, sha: "a".repeat(40) + "\n" },
      { ...source, tree: "bad" },
    ]) {
      expect(readAppIdentity({ version: "1.0.0", jonesSource }).source).toBeNull();
    }
  });

  it.each(["Codex", "Claude Code", "Cursor", "OpenCode", "Grok", "Antigravity"])(
    "injects explicit server identity through the shared %s runtime context",
    (harness) => {
      const prompt = buildRuntimeInstructions({ harness });
      expect(prompt).toContain(`running in Jones Code through the ${harness} harness`);
      expect(prompt).toContain(`product ID: jones-code; server version: ${APP_IDENTITY.version}`);
      expect(prompt).toContain("get_invocation_context");
      expect(prompt).toContain("identity is unknown");
      expect(prompt).not.toContain("running in T3 Code");
      expect(
        buildCodexAdditionalContext({ model: "gpt-5.4", reasoningEffort: "medium" }).t3_code_runtime
          ?.value,
      ).toContain("product ID: jones-code");
      expect(T3_CODE_BROWSER_TOOL_INSTRUCTIONS).toContain("You are running inside Jones Code.");
    },
  );

  it.effect("keeps Codex identity when MCP is disabled without advertising attached tools", () =>
    Effect.gen(function* () {
      const turn = yield* buildCodexTurnStartParams({
        nativeThreadId: "synthetic",
        codexInput: [{ type: "text", text: "hello" }],
        runtimePolicy: { runtimeMode: "full-access", interactionMode: "default", cwd: null },
        modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" },
        hasT3Mcp: false,
      });
      expect(turn.additionalContext?.jones_code_identity?.value).toContain(
        "product ID: jones-code",
      );
      expect(turn.additionalContext).not.toHaveProperty("t3_code_orchestration");
      expect(turn.additionalContext).not.toHaveProperty("t3_code_tools");
      expect(turn.collaborationMode).toBeUndefined();
    }),
  );

  it("Pi injects identity on every start with MCP disabled, preserving its original prompt", async () => {
    const handlers = new Map<
      string,
      (event: { systemPrompt: string }) => { systemPrompt: string }
    >();
    const extension = NodeModule.stripTypeScriptTypes(
      PI_T3_MCP_EXTENSION_SOURCE.replace('import { Type } from "typebox";', "").replace(
        "export default async function",
        "async function",
      ),
    );
    await NodeVM.runInNewContext(`${extension}\nt3McpExtension(pi)`, {
      process: { env: {} },
      pi: {
        on: (
          name: string,
          handler: (event: { systemPrompt: string }) => { systemPrompt: string },
        ) => handlers.set(name, handler),
      },
    });
    const hook = handlers.get("before_agent_start")!;
    expect(hook).toBeDefined();
    for (const systemPrompt of ["original prompt", "resumed prompt"]) {
      const result = hook({ systemPrompt }).systemPrompt;
      expect(result).toContain(`${systemPrompt}\n\nApp identity: Jones Code`);
      expect(result).not.toContain("## Jones Code orchestration");
    }
  });
});
