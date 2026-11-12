import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { WorkstreamAction, WorkstreamCommand } from "../workstreams.ts";
import * as Operations from "./workstreamOperations.ts";

const isOperationRequest = Schema.is(Operations.WorkstreamOperationRequest);
const isCreateRequest = Schema.is(Operations.WorkstreamCreateRequest);
const isWorkstreamAction = Schema.is(WorkstreamAction);

const request = {
  contract: Operations.WORKSTREAM_OPERATIONS_CONTRACT,
  operation_id: "operation-000001",
  origin: { kind: "app" },
  kind: "set_primary",
  thread: { environment_id: "env-1", thread_id: "thread-1" },
  target: {
    kind: "workstream",
    registry_id: "registry-1",
    owner_id: "owner-1",
    workstream_id: "ws-1",
  },
  expected: { kind: "unassigned" },
};
const createRequest = {
  contract: request.contract,
  operation_id: request.operation_id,
  origin: request.origin,
  kind: "create_workstream",
  registry: { registry_id: "registry-1", owner_id: "owner-1" },
  workstream: {
    name: "Synthetic workstream",
    lifecycle: "active",
    progress: { state: "unknown" },
    sort_order: 0,
  },
};

describe("workstream operation requests", () => {
  it("round trips assignment, unassignment and create without parse options", () => {
    for (const value of [
      request,
      createRequest,
      {
        ...request,
        target: { kind: "unassigned", registry_id: "registry-1", owner_id: "owner-1" },
      },
    ])
      expect(Operations.parseWorkstreamOperationRequest(value)).toEqual(value);
  });

  it("requires queue assignments to compare against unassigned", () => {
    const queued = { ...request, origin: { kind: "queue", request_id: "request-1" } };
    expect(Operations.parseWorkstreamOperationRequest(queued)).toEqual(queued);
    for (const expected of [{ kind: "any" }, { kind: "primary", workstream_id: "ws-1" }]) {
      expect(() => Operations.parseWorkstreamOperationRequest({ ...queued, expected })).toThrow();
    }
  });

  it("rejects undeclared fields at every request object boundary", () => {
    const decode = Operations.parseWorkstreamOperationRequest;
    expect(() => decode({ ...request, wait_ms: 0 })).toThrow();
    for (const field of ["origin", "thread", "target", "expected"] as const) {
      expect(() => decode({ ...request, [field]: { ...request[field], extra: true } })).toThrow();
    }
    for (const field of ["registry", "workstream"] as const) {
      expect(() =>
        decode({ ...createRequest, [field]: { ...createRequest[field], extra: true } }),
      ).toThrow();
    }
    for (const progress of [
      { state: "progressing", extra: true },
      { state: "waiting", condition: "Ready", extra: true },
      { state: "blocked", impediment: "Dependency", extra: true },
    ]) {
      expect(() =>
        decode({ ...createRequest, workstream: { ...createRequest.workstream, progress } }),
      ).toThrow();
    }
  });

  it("uses exactly the existing CommandId validation", () => {
    const original = Schema.is(WorkstreamCommand.fields.command_id);
    for (const operation_id of [
      "a".repeat(15),
      "a".repeat(16),
      "a".repeat(128),
      "a".repeat(129),
      "-".repeat(16),
      "abc.def:ghi_jkl-0",
      "a".repeat(16) + "\n",
    ]) {
      expect(isOperationRequest({ ...request, operation_id })).toBe(original(operation_id));
    }
  });

  it("preserves create-action field validation including boundary values", () => {
    for (const workstream of [
      createRequest.workstream,
      { ...createRequest.workstream, name: " " },
      { ...createRequest.workstream, name: "a".repeat(160) },
      { ...createRequest.workstream, name: "a".repeat(161) },
      { ...createRequest.workstream, sort_order: 2_147_483_647 },
      { ...createRequest.workstream, sort_order: 2_147_483_648 },
      { ...createRequest.workstream, sort_order: -1 },
      { ...createRequest.workstream, sort_order: 0.5 },
      { ...createRequest.workstream, lifecycle: "invalid" },
      { ...createRequest.workstream, progress: { state: "waiting", condition: " " } },
      { ...createRequest.workstream, progress: { state: "blocked", impediment: "dependency" } },
    ]) {
      expect(isCreateRequest({ ...createRequest, workstream })).toBe(
        isWorkstreamAction({ operation: "create_workstream", ...workstream }),
      );
    }
  });

  it("bounds submit waits and keeps the request separate from its envelope", () => {
    const decode = Schema.decodeUnknownSync(Operations.WorkstreamOperationSubmitRequest);
    for (const wait_ms of [0, 3000])
      expect(decode({ request, wait_ms })).toEqual({ request, wait_ms });
    for (const wait_ms of [-1, 3001, 0.5, "0", null])
      expect(() => decode({ request, wait_ms })).toThrow();
    expect(() => decode({ request, wait_ms: 0, request_sha256: "a".repeat(64) })).toThrow();
  });

  it("requires exactly one read selector", () => {
    const decode = Schema.decodeUnknownSync(Operations.WorkstreamOperationReadRequest);
    for (const value of [
      { operation_id: request.operation_id },
      { thread: request.thread },
      { open: true },
    ])
      expect(decode(value)).toEqual(value);
    for (const value of [
      {},
      { open: false },
      { open: true, operation_id: request.operation_id },
      { thread: { ...request.thread, extra: true } },
    ])
      expect(() => decode(value)).toThrow();
  });

  it("limits conflict errors to the declared codes and optional command ID", () => {
    const decode = Schema.decodeUnknownSync(Operations.EnvironmentWorkstreamOperationConflictError);
    for (const value of [
      { code: "operation_id_conflict" },
      { code: "thread_operation_in_progress", open_operation_id: request.operation_id },
    ])
      expect(decode(value)).toEqual(value);
    for (const value of [
      { code: "conflict" },
      { code: "operation_id_conflict", open_operation_id: "short" },
      { code: "operation_id_conflict", extra: true },
    ])
      expect(() => decode(value)).toThrow();
  });
});

// @effect-diagnostics-next-line nodeBuiltinImport:off -- Verify immutable corpus bytes without a runtime service.
import * as NodeCrypto from "node:crypto";
import manifest from "../../contracts/workstream-operations/v1/manifest.json" with { type: "json" };
import conformance from "../../contracts/workstream-operations/v1/fixtures/conformance.json" with { type: "json" };
import negatives from "../../contracts/workstream-operations/v1/fixtures/negative-cases.json" with { type: "json" };
import semantics from "../../contracts/workstream-operations/v1/fixtures/semantic-cases.json" with { type: "json" };
import readmeRaw from "../../contracts/workstream-operations/v1/README.md?raw";
import requestRaw from "../../contracts/workstream-operations/v1/schemas/operation-request.schema.json?raw";
import resultRaw from "../../contracts/workstream-operations/v1/schemas/operation-result.schema.json?raw";
import setupRaw from "../../contracts/workstream-operations/v1/schemas/setup-status.schema.json?raw";
import conformanceRaw from "../../contracts/workstream-operations/v1/fixtures/conformance.json?raw";
import negativeRaw from "../../contracts/workstream-operations/v1/fixtures/negative-cases.json?raw";
import semanticRaw from "../../contracts/workstream-operations/v1/fixtures/semantic-cases.json?raw";

const rawFiles: Readonly<Record<string, string>> = {
  "README.md": readmeRaw,
  "schemas/operation-request.schema.json": requestRaw,
  "schemas/operation-result.schema.json": resultRaw,
  "schemas/setup-status.schema.json": setupRaw,
  "fixtures/conformance.json": conformanceRaw,
  "fixtures/negative-cases.json": negativeRaw,
  "fixtures/semantic-cases.json": semanticRaw,
};
const decoders = new Map<string, (value: unknown) => unknown>([
  ["Request", Operations.parseWorkstreamOperationRequest],
  ["SubmitRequest", Schema.decodeUnknownSync(Operations.WorkstreamOperationSubmitRequest)],
  ["ReadRequest", Schema.decodeUnknownSync(Operations.WorkstreamOperationReadRequest)],
  [
    "ConflictError",
    Schema.decodeUnknownSync(Operations.EnvironmentWorkstreamOperationConflictError),
  ],
  ["Result", Operations.parseWorkstreamOperationResult],
  ["SubmitResponse", Schema.decodeUnknownSync(Operations.WorkstreamOperationSubmitResponse)],
  ["ReadResponse", Schema.decodeUnknownSync(Operations.WorkstreamOperationReadResponse)],
  ["SetupStatus", Operations.parseWorkstreamSetupStatus],
]);
const decodeFixture = (fixture: { schema: string; value: unknown }) => {
  const decode = decoders.get(fixture.schema.slice(fixture.schema.lastIndexOf("/") + 1));
  if (decode === undefined) throw new Error(`Uncovered schema ${fixture.schema}`);
  return decode(fixture.value);
};
const sha256 = (bytes: string) => NodeCrypto.createHash("sha256").update(bytes).digest("hex");

describe("workstream-operations/1.0.0 pinned corpus", () => {
  it("binds the complete sorted corpus and family version", () => {
    expect(manifest.files.map((file) => file.path)).toEqual(Object.keys(rawFiles).sort());
    expect(manifest.contract_family).toBe(Operations.WORKSTREAM_OPERATIONS_FAMILY);
    expect(manifest.contract_version).toBe(Operations.WORKSTREAM_OPERATIONS_VERSION);
    expect(manifest.digest_algorithm).toBe("sha256");
    expect(manifest.manifest_sha256).toBe(Operations.WORKSTREAM_OPERATIONS_MANIFEST_SHA256);
    expect(sha256(manifest.files.map(({ path, sha256 }) => `${sha256}  ${path}\n`).join(""))).toBe(
      manifest.manifest_sha256,
    );
    for (const corpus of [conformance, negatives, semantics]) {
      expect(corpus.contract_family).toBe(manifest.contract_family);
      expect(corpus.contract_version).toBe(manifest.contract_version);
      expect(new Set(corpus.cases.map((entry) => entry.name)).size).toBe(corpus.cases.length);
    }
  });
  it.each(manifest.files)("pins exact bytes for $path", ({ path, sha256: expected }) => {
    expect(sha256(rawFiles[path]!)).toBe(expected);
  });
  it.each(conformance.cases)("accepts $name without parse options", (fixture) => {
    expect(decodeFixture(fixture)).toEqual(fixture.value);
  });
  it.each(negatives.cases)("rejects structural case $name", (fixture) => {
    expect(() => decodeFixture(fixture)).toThrow();
  });
  it.each(semantics.cases)("rejects semantic case $name", (fixture) => {
    expect(() => decodeFixture(fixture)).toThrow();
  });
});
