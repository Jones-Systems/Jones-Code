import * as NodeAssert from "node:assert/strict";

import {
  T3_CODE_BROWSER_TOOL_INSTRUCTIONS,
  T3_CODE_DEVICE_TOOL_INSTRUCTIONS,
} from "@t3tools/provider-core/server/orchestrationInstructions";
import { describe, it } from "vite-plus/test";
import {
  T3_CODE_ORCHESTRATION_INSTRUCTIONS,
  T3_CODE_ORCHESTRATION_SECTIONS,
} from "@t3tools/provider-core/server/orchestrationInstructions";

import {
  buildCodexAdditionalContext,
  CODEX_ADDITIONAL_CONTEXT_VALUE_MAX_BYTES,
} from "./CodexDeveloperInstructions.ts";

describe("buildCodexAdditionalContext", () => {
  describe.each([
    { model: "gpt-5.3-codex", reasoningEffort: "high" },
    { model: "custom-model", modelName: "Custom 🦊 Model", reasoningEffort: "xhigh" },
  ])("with runtime $model", (runtime) => {
    it.each([
      undefined,
      false,
      true,
      { browser: false, device: false },
      { browser: true, device: false },
      { browser: false, device: true },
      { browser: true, device: true },
    ])("keeps every entry within Codex's per-entry cap with tools %j", (availability) => {
      const context = buildCodexAdditionalContext(runtime, availability);

      for (const [key, entry] of Object.entries(context)) {
        // Codex counts ceil(UTF-8 bytes / 4) tokens, excluding keys and wrappers.
        const bytes = Buffer.byteLength(entry.value, "utf8");
        const limit = CODEX_ADDITIONAL_CONTEXT_VALUE_MAX_BYTES;
        NodeAssert.ok(
          bytes <= limit,
          `${key}: ${bytes} UTF-8 bytes exceeds Codex's ${limit}-byte per-entry cap by ${bytes - limit} bytes; split the instructions into separate entries to avoid silent truncation`,
        );
      }
    });
  });
});

describe("Codex runtime context", () => {
  it("describes Markdown media support in the runtime context in both modes", () => {
    {
      const instructions = runtimeInstructions({
        model: "gpt-5.3-codex",
        reasoningEffort: "high",
      });
      NodeAssert.match(
        instructions,
        /<runtime_info>.*embed images, video, and audio.*Markdown image syntax with absolute file paths.*Link any other file.*absolute path.*files viewer.*<\/runtime_info>/,
      );
    }
  });

  it("flattens multiline metadata into single-line runtime info", () => {
    const instructions = runtimeInstructions({
      model: "gpt\n5.3\ncodex",
      reasoningEffort: " high\neffort ",
    });

    NodeAssert.match(instructions, /as gpt 5\.3 codex with high effort reasoning effort/);
    NodeAssert.doesNotMatch(instructions, /<runtime_info>[^<]*\n/);
  });
});

describe("Codex orchestration context", () => {
  it("preserves Jones delegation ownership and model options in Codex context", () => {
    const context = buildCodexAdditionalContext({
      model: "gpt-5.3-codex",
      reasoningEffort: "high",
    });
    const sections = Object.entries(context)
      .filter(([key]) => key.startsWith("t3_code_orchestration_"))
      .map(([, entry]) => entry);
    const orchestration = Object.entries(buildCodexAdditionalContext({ model: "gpt-6.1-sol" }))
      .filter(([key]) => key.startsWith("t3_code_orchestration_"))
      .map(([, entry]) => entry.value)
      .join("\n");

    NodeAssert.ok(
      orchestration.includes(
        "Use `delegate_task` for every Claude child, including same-provider work such as Claude to Claude: " +
          "when you are a Claude agent asked to make subagents, or the child should be a Claude agent, " +
          "create app-owned `delegate_task` children with a Claude target even when a native Claude subagent tool supports the chosen model.",
      ),
    );

    NodeAssert.ok(
      orchestration.includes(
        "For other providers, prefer native subagent tools for same-provider work only when they support the chosen model " +
          "and no model options are requested or inherited, and neither the user nor the workstream asks for a separate session " +
          "or rules out native subagents; use `delegate_task` with that provider instance and model in every other case.",
      ),
    );

    NodeAssert.ok(
      orchestration.includes(
        "Also use `delegate_task` for cross-provider or explicitly T3-owned child tasks.",
      ),
    );

    NodeAssert.ok(
      orchestration.includes(
        "When model options are requested or the child keeps the parent's provider instance and model with nonempty `inheritedOptions` " +
          "from `orchestrator_capabilities`, use `delegate_task` even for native-supported models. " +
          "Native subagents cannot reliably apply T3 model options. " +
          "Read the selected model's descriptors from `orchestrator_capabilities` and put settings in `target.options`, not only in the prompt. " +
          'Claude commonly uses {"effort":"max","fastMode":true}; Codex uses {"reasoningEffort":"max","serviceTier":"priority"}. ' +
          "Do not translate max to high or ignore unsupported settings: report a capability mismatch. " +
          "Options inherit only for the same provider instance and model; explicit entries override individual values, and [] or {} clears them. " +
          "Check the returned providerInstanceId, model, and options against the requested selection.",
      ),
    );

    NodeAssert.ok(
      orchestration.includes(
        "For an independent T3 delegated review round, call `delegate_task` again. " +
          "Include the original brief, prior findings, responses, and unresolved objections in the new task prompt. " +
          "Track each round by its own `taskId` and use a distinct `clientRequestId`, stable across retries of that round.",
      ),
    );
    NodeAssert.equal(
      sections.map((entry) => entry.value).join("\n\n"),
      T3_CODE_ORCHESTRATION_INSTRUCTIONS.trim(),
    );
    NodeAssert.match(context.t3_code_runtime!.value, /running in Jones Code/);
    NodeAssert.match(context.t3_code_runtime!.value, /product ID: jones-code/);
    NodeAssert.match(
      context.t3_code_runtime!.value,
      /as gpt-5\.3-codex with high reasoning effort/,
    );
  });
  it("delivers all authored sections in key order with the actual app identity", () => {
    const context = buildCodexAdditionalContext({
      model: "gpt-5.3-codex",
      reasoningEffort: "high",
    });
    const sections = Object.keys(context)
      .filter((key) => key.startsWith("t3_code_orchestration_"))
      .sort()
      .map((key) => context[key]!);
    NodeAssert.deepEqual(
      sections,
      T3_CODE_ORCHESTRATION_SECTIONS.map((value) => ({ kind: "application", value })),
    );
    NodeAssert.equal(
      sections.map((entry) => entry.value).join("\n\n"),
      T3_CODE_ORCHESTRATION_INSTRUCTIONS.trim(),
    );
    NodeAssert.match(context.t3_code_runtime!.value, /running in Jones Code/);
    NodeAssert.match(context.t3_code_runtime!.value, /product ID: jones-code/);
    NodeAssert.match(
      context.t3_code_runtime!.value,
      /as gpt-5\.3-codex with high reasoning effort/,
    );
  });
});

describe("Jones browser developer instructions", () => {
  const runtime = { model: "gpt-5.3-codex", reasoningEffort: "high" };
  it("prefers attached product-native preview tools", () => {
    const instructions = buildCodexAdditionalContext(runtime, true).t3_code_tools!.value;
    for (const text of [
      "Jones Code collaborative browser",
      "preview_status",
      "preview_open",
      "Do not switch to global browser skills",
    ])
      NodeAssert.ok(instructions.includes(text));
  });
  it("tracks browser and device MCP configuration per turn", () => {
    NodeAssert.equal(buildCodexAdditionalContext(runtime, false).t3_code_tools, undefined);
    const deviceOnly = buildCodexAdditionalContext(runtime, { browser: false, device: true })
      .t3_code_tools!.value;
    NodeAssert.doesNotMatch(deviceOnly, /preview_open/);
    NodeAssert.match(deviceOnly, /device_open/);
    const browserOnly = buildCodexAdditionalContext(runtime, { browser: true, device: false })
      .t3_code_tools!.value;
    NodeAssert.match(browserOnly, /preview_open/);
    NodeAssert.doesNotMatch(browserOnly, /device_open/);
  });
});

describe("Codex device context", () => {
  it("delivers both complete tool blocks when their combined value exceeds the native cap", () => {
    const context = buildCodexAdditionalContext(
      { model: "custom-model" },
      { browser: true, device: true },
    );
    const toolValues = Object.entries(context)
      .filter(([key]) => key.startsWith("t3_code_tools"))
      .map(([, entry]) => entry.value);
    const delivered = toolValues.join("\n\n");
    NodeAssert.ok(delivered.includes(T3_CODE_BROWSER_TOOL_INSTRUCTIONS));
    NodeAssert.ok(delivered.includes(T3_CODE_DEVICE_TOOL_INSTRUCTIONS));
    NodeAssert.equal(delivered.split(T3_CODE_BROWSER_TOOL_INSTRUCTIONS).length - 1, 1);
    NodeAssert.equal(delivered.split(T3_CODE_DEVICE_TOOL_INSTRUCTIONS).length - 1, 1);
  });
  it("adds the shared device block only when the session holds device access", () => {
    const tools = (device: boolean) =>
      buildCodexAdditionalContext({ model: "gpt-6.1-sol" }, { browser: true, device }).t3_code_tools
        ?.value ?? "";

    NodeAssert.ok(tools(true).includes(T3_CODE_DEVICE_TOOL_INSTRUCTIONS));
    NodeAssert.ok(!tools(true).includes("ToolSearch"));
    NodeAssert.ok(!tools(false).includes("## T3 Code devices"));
  });
});

function runtimeInstructions(runtime: Parameters<typeof buildCodexAdditionalContext>[0]) {
  return buildCodexAdditionalContext(runtime).t3_code_runtime!.value;
}
