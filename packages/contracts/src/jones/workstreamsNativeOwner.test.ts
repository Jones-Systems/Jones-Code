import { expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import {
  WorkstreamsNativeOwnerRequest,
  WorkstreamsNativeOwnerResponse,
} from "./workstreamsNativeOwner.ts";

const decode = Schema.decodeUnknownSync(WorkstreamsNativeOwnerRequest);
it("accepts only the five explicit owner operations without caller-selected locations", () => {
  for (const request of [
    { operation: "authority-enroll" },
    {
      operation: "provider-prepare",
      enrollmentId: "laptop",
      ownerId: "owner",
      principalId: "principal",
      registryOrigin: "https://registry.example",
      lifetimeDays: 30,
    },
    { operation: "provider-plan", enrollmentId: "laptop" },
    { operation: "provider-apply", enrollmentId: "laptop", expectedPriorSha256: null },
    {
      operation: "provider-readback",
      enrollmentId: "laptop",
      expectedCredentialSha256: "a".repeat(64),
    },
  ]) {
    expect(decode(request)).toEqual(request);
    expect(() => decode({ ...request, baseDir: "/other" })).toThrow();
    expect(() => decode({ ...request, credentialPath: "/other" })).toThrow();
  }
  expect(() => decode({ operation: "provider-plan", enrollmentId: "../other" })).toThrow();
  expect(() => decode({ operation: "restore" })).toThrow();
});
it("never accepts a credential in an owner response", () => {
  const response = {
    schema: "jones-code.workstreams-native-authority-enrolled/v1",
    environment_id: "env",
    authority_namespace: "t3-native:fixture",
    store_generation: 1,
  };
  expect(Schema.decodeUnknownSync(WorkstreamsNativeOwnerResponse)(response)).toEqual(response);
  expect(() =>
    Schema.decodeUnknownSync(WorkstreamsNativeOwnerResponse)({
      ...response,
      authorization_header: "Bearer secret",
    }),
  ).toThrow();
});
