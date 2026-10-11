import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";

import { EnvironmentAuthenticatedAuth, EnvironmentHttpApi } from "../environmentHttp.ts";
import { JonesNativePrepareInput, JonesUpdateInstallInput } from "./jonesUpdates.ts";

// The public API metadata is also consumed by independently upgraded clients.
// Keep the Jones routes and authentication identity stable through relocation.
describe("Jones environment HTTP registration", () => {
  it("preserves the environment group order", () => {
    expect(Object.keys(EnvironmentHttpApi.groups)).toEqual([
      "providerQueue",
      "queueDispatch",
      "voiceReview",
      "hostStatus",
      "workQueueMetadata",
      "metadata",
      "auth",
      "mcpOAuth",
      "orchestration",
      "pullRequests",
      "workstreams",
      "workstreamsNative",
      "projects",
      "connect",
      "conversationLibrary",
      "jonesUpdates",
      "fleetUpdates",
      "jonesImportedHistory",
      "previewCompanion",
      "webhooks",
    ]);
  });

  it("preserves Jones route methods, paths, and authentication on every endpoint", () => {
    const groups = EnvironmentHttpApi.groups;
    const routes = [
      ...Object.values(groups.voiceReview.endpoints),
      ...Object.values(groups.hostStatus.endpoints),
      ...Object.values(groups.workQueueMetadata.endpoints),
      ...Object.values(groups.conversationLibrary.endpoints),
      ...Object.values(groups.workstreams.endpoints),
      ...Object.values(groups.jonesUpdates.endpoints),
      ...Object.values(groups.workstreamsNative.endpoints),
    ];
    expect(routes.map(({ identifier, method, path }) => [identifier, method, path])).toEqual([
      ["recent", "GET", "/api/voice-review/recent"],
      ["registrySnapshot", "GET", "/api/voice-review/registry/snapshot"],
      ["registryWorkstreams", "GET", "/api/voice-review/registry/workstreams"],
      ["registryEvents", "GET", "/api/voice-review/registry/events"],
      ["correctAssociation", "POST", "/api/voice-review/registry/associations"],
      ["correctLabel", "POST", "/api/voice-review/registry/labels"],
      ["diagnostics", "GET", "/api/voice-review/drafts/:id/diagnostics"],
      ["list", "GET", "/api/voice-review/drafts"],
      ["get", "GET", "/api/voice-review/drafts/:id"],
      ["pause", "POST", "/api/voice-review/drafts/:id/pause"],
      ["play", "POST", "/api/voice-review/drafts/:id/play"],
      ["editBegin", "POST", "/api/voice-review/drafts/:id/edit-begin"],
      ["editSave", "POST", "/api/voice-review/drafts/:id/edit-save"],
      ["editCancel", "POST", "/api/voice-review/drafts/:id/edit-cancel"],
      ["sendNow", "POST", "/api/voice-review/drafts/:id/send-now"],
      ["delete", "POST", "/api/voice-review/drafts/:id/delete"],
      ["snapshot", "GET", "/api/host-status"],
      ["snapshot", "GET", "/api/work-queue/metadata"],
      ["conversationLibrary", "POST", "/api/conversation-library"],
      ["appearanceRead", "POST", "/api/workstreams/appearance/read"],
      ["appearanceSave", "POST", "/api/workstreams/appearance/write"],
      ["registrationContext", "GET", "/api/workstreams/registration-context"],
      ["threadPlacements", "POST", "/api/workstreams/thread-placements"],
      ["list", "GET", "/api/workstreams"],
      ["references", "GET", "/api/workstreams/references"],
      ["reference", "GET", "/api/workstreams/references/:nativeReferenceId"],
      ["detail", "GET", "/api/workstreams/:workstreamId"],
      ["memberships", "GET", "/api/workstreams/:workstreamId/memberships"],
      ["declarations", "GET", "/api/workstreams/:workstreamId/declarations"],
      ["edges", "GET", "/api/workstreams/:workstreamId/edges"],
      ["history", "GET", "/api/workstreams/:workstreamId/history"],
      ["command", "GET", "/api/workstreams/commands/:commandId"],
      ["submit", "POST", "/api/workstreams/commands"],
      ["prepareNative", "POST", "/api/jones-updates/prepare-native"],
      ["state", "GET", "/api/jones-updates"],
      ["check", "POST", "/api/jones-updates/check"],
      ["download", "POST", "/api/jones-updates/download"],
      ["install", "POST", "/api/jones-updates/install"],
      ["context", "GET", "/api/workstreams/native/v1/context"],
      ["attestations", "POST", "/api/workstreams/native/v1/attestations"],
      ["settlements", "POST", "/api/workstreams/native/v1/settlements"],
      ["settlementLookup", "POST", "/api/workstreams/native/v1/settlements/lookup"],
    ]);
    const bearerHeaders = Schema.Struct({
      authorization: Schema.optionalKey(Schema.String),
      dpop: Schema.optionalKey(Schema.String),
    });
    for (const endpoint of routes) {
      expect(endpoint.middlewares.has(EnvironmentAuthenticatedAuth)).toBe(true);
      expect(endpoint.headers?.ast).toEqual(bearerHeaders.ast);
    }
  });
});

it("requires an explicit native attempt without changing ordinary install requests", () => {
  const base = { stagedHandle: "a".repeat(64), environmentId: "fixture", currentVersion: "1.0.0" };
  const decode = Schema.decodeUnknownSync(JonesNativePrepareInput);
  expect(() => decode(base)).toThrow();
  expect(() => decode({ ...base, transactionId: "invalid" })).toThrow();
  expect(decode({ ...base, transactionId: "b".repeat(64) })).toEqual({
    ...base,
    transactionId: "b".repeat(64),
  });
  expect(Schema.decodeUnknownSync(JonesUpdateInstallInput)(base)).toEqual(base);
});
