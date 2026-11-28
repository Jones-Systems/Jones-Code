import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { ExecutionEnvironmentCapabilities } from "../environment.ts";
import { OrchestrationV2ThreadLaunchInput } from "../orchestrationV2.ts";

const concreteLaunch = {
  commandId: "launch-account-routing",
  projectId: "project",
  title: "Start a task",
  modelSelection: { instanceId: "codex", model: "gpt-6.2-sol" },
  runtimeMode: "full-access",
  interactionMode: "default",
  workspaceStrategy: { type: "root" },
};

const decodeLaunch = Schema.decodeUnknownSync(OrchestrationV2ThreadLaunchInput);
const decodeCapabilities = Schema.decodeUnknownSync(ExecutionEnvironmentCapabilities);
const { accountAutoRouting: _auto, ...legacyFields } = ExecutionEnvironmentCapabilities.fields;
const decodeLegacyCapabilities = Schema.decodeUnknownSync(Schema.Struct(legacyFields));

describe("automatic account routing compatibility", () => {
  it("accepts legacy explicit selections and decodes the launch-only intent", () => {
    expect(decodeLaunch(concreteLaunch)).toEqual(concreteLaunch);
    const accountRouting = { mode: "auto" };
    const launch = { ...concreteLaunch, accountRouting };
    expect(decodeLaunch(launch)).toEqual(launch);
    expect(() => decodeLaunch({ ...launch, modelSelection: { model: "gpt-6.2-sol" } })).toThrow();
    expect(() => decodeLaunch({ ...concreteLaunch, accountRouting: { mode: "rotate" } })).toThrow();
  });

  it("defaults old descriptions to no capability and preserves explicit advertisement", () => {
    expect(decodeCapabilities({}).accountAutoRouting).toBeUndefined();
    expect(decodeCapabilities({ accountAutoRouting: true }).accountAutoRouting).toBe(true);
    expect(decodeCapabilities({ accountAutoRouting: false }).accountAutoRouting).toBe(false);
    expect(decodeLegacyCapabilities({ accountAutoRouting: true })).toEqual(decodeCapabilities({}));
  });
});
