import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";

import { EnvironmentAuthenticatedAuth, EnvironmentHttpApi } from "../environmentHttp.ts";

// The public API metadata is also consumed by independently upgraded clients.
// Keep the Jones routes and authentication identity stable through relocation.
describe("Jones environment HTTP registration", () => {
  it("preserves the environment group order", () => {
    expect(Object.keys(EnvironmentHttpApi.groups)).toEqual([
      "voiceReview",
      "hostStatus",
      "metadata",
      "auth",
      "orchestration",
      "pullRequests",
      "projects",
      "connect",
      "conversationLibrary",
    ]);
  });

  it("preserves Jones route methods, paths, and authentication on every endpoint", () => {
    const groups = EnvironmentHttpApi.groups;
    const routes = [
      ...Object.values(groups.voiceReview.endpoints),
      ...Object.values(groups.hostStatus.endpoints),
      ...Object.values(groups.conversationLibrary.endpoints),
    ];
    expect(routes.map(({ identifier, method, path }) => [identifier, method, path])).toEqual([
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
      ["conversationLibrary", "POST", "/api/conversation-library"],
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
