import { expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";
import { enrollmentRequest } from "../../enrollment/testFixtures.ts";
import {
  NativeSourceSetupRequest,
  NativeSourceEnrollmentV1,
  sourceSetupDigest,
} from "./sourceEnrollment.ts";

const request: NativeSourceSetupRequest = {
  schema: "jones-code.native-source-setup/v1",
  enrollment: enrollmentRequest,
  expected: {
    registry_instance_id: "registry",
    owner_id: enrollmentRequest.context.owner_id,
    home_environment_id: enrollmentRequest.context.source_instance_id,
    home_generation: 1,
    registry_version: 4,
    singleton_binding_version: 1,
    grant_id: "grant",
    grant_version: 1,
    source_binding: null,
  },
  rebind: false,
};
it("closed native issuance versions reject extras and mismatched digest, receipt or owner", () => {
  const decode = Schema.decodeUnknownSync(NativeSourceSetupRequest);
  expect(decode(request)).toEqual(request);
  for (const bad of [
    { ...request, secret: "forbidden" },
    { ...request, expected: { ...request.expected, extra: 1 } },
    { ...request, expected: { ...request.expected, owner_id: "other" } },
    { ...request, schema: "unknown" },
  ])
    expect(() => decode(bad)).toThrow();
  const proof = {
    schema: "jones-code.native-source-enrollment/v1",
    request,
    request_sha256: sourceSetupDigest(request),
    receipt: { registry_version: 5, source_binding_version: 1 },
  };
  const read = Schema.decodeUnknownSync(NativeSourceEnrollmentV1);
  expect(read(proof)).toEqual(proof);
  for (const bad of [
    { ...proof, request_sha256: "f".repeat(64) },
    { ...proof, receipt: { registry_version: 6, source_binding_version: 1 } },
    { ...proof, receipt: { registry_version: 5, source_binding_version: 2 } },
    { ...proof, token: "forbidden" },
  ])
    expect(() => read(bad)).toThrow();
  expect(sourceSetupDigest({ ...request, rebind: true })).not.toBe(sourceSetupDigest(request));
});
