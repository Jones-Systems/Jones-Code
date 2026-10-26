import * as NodeAssert from "node:assert/strict";

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
    const orchestration = Object.entries(buildCodexAdditionalContext({ model: "gpt-6.1-sol" }))
      .filter(([key]) => key.startsWith("t3_code_orchestration_"))
      .map(([, entry]) => entry.value)
      .join("\n");

    NodeAssert.ok(orchestration.includes("For other providers, prefer native subagent tools"));
    NodeAssert.ok(orchestration.includes("Use `delegate_task` for every Claude child"));
    NodeAssert.ok(orchestration.includes("inheritedOptions"));
    NodeAssert.ok(orchestration.includes("For an independent T3 delegated review round"));
    NodeAssert.ok(orchestration.includes("Do not use `t3_thread_send` on `childThreadId`"));
    NodeAssert.ok(orchestration.includes("Follow a running delegate's `next` instruction"));
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

function runtimeInstructions(runtime: Parameters<typeof buildCodexAdditionalContext>[0]) {
  return buildCodexAdditionalContext(runtime).t3_code_runtime!.value;
}
