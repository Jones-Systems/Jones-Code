import { expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";
import { enrolledContext } from "../nativeProvider/testFixtures.ts";
import { enrollmentRequest } from "./testFixtures.ts";
import {
  NativeEnrollmentRequest,
  canonicalNativeEnrollmentRequest,
  deriveNativeEnrollmentSession,
  nativeEnrollmentRequestSha256,
} from "./request.ts";

it("canonical enrollment digest includes every immutable request field and ignores key insertion order", () => {
  const reordered = {
    session: enrollmentRequest.session,
    context: {
      ...enrollmentRequest.context,
      build: {
        tree: enrolledContext.build.tree,
        sha: enrolledContext.build.sha,
        repository: enrolledContext.build.repository,
      },
    },
    registry_origin: enrollmentRequest.registry_origin,
    enrollment_id: enrollmentRequest.enrollment_id,
    schema: enrollmentRequest.schema,
  };
  expect(nativeEnrollmentRequestSha256(reordered)).toBe(
    nativeEnrollmentRequestSha256(enrollmentRequest),
  );
  expect(
    nativeEnrollmentRequestSha256({
      ...enrollmentRequest,
      registry_origin: "https://other.invalid",
    }),
  ).not.toBe(nativeEnrollmentRequestSha256(enrollmentRequest));
  expect(canonicalNativeEnrollmentRequest(enrollmentRequest)).not.toContain("\n");
  const expected = deriveNativeEnrollmentSession(enrollmentRequest);
  expect(expected.subject).toBe(`workstreams-native:${enrollmentRequest.enrollment_id}`);
  expect(expected.method).toBe("bearer-access-token");
  expect(expected.client.deviceType).toBe("bot");
  expect(expected.scopes).toEqual([
    "workstreams:native:context",
    "workstreams:native:settlement",
    "workstreams:native:reconciliation",
  ]);
});

it("closed enrollment rejects extra fields, noncanonical origins, changed identity and invalid lifetimes", () => {
  const decode = Schema.decodeUnknownSync(NativeEnrollmentRequest);
  for (const candidate of [
    { ...enrollmentRequest, token: "private" },
    { ...enrollmentRequest, session: { ...enrollmentRequest.session, ttl: 30 } },
    { ...enrollmentRequest, enrollment_id: "different" },
    ...[
      "http://registry.invalid",
      "https://registry.invalid/",
      "https://registry.invalid:443",
      "https://user@registry.invalid",
      "https://REGISTRY.invalid",
    ].map((registry_origin) => ({ ...enrollmentRequest, registry_origin })),
    {
      ...enrollmentRequest,
      session: { ...enrollmentRequest.session, expires_at: "2026-02-01T00:00:00.000Z" },
    },
    {
      ...enrollmentRequest,
      session: { ...enrollmentRequest.session, expires_at: "2026-01-01T00:00:00.000Z" },
    },
    {
      ...enrollmentRequest,
      session: { ...enrollmentRequest.session, issued_at: "2026-02-30T00:00:00.000Z" },
    },
  ])
    expect(() => decode(candidate)).toThrow();
});
