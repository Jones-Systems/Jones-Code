// @effect-diagnostics nodeBuiltinImport:off -- Independent test hashes bind scripted CP receipts to exact bytes.
import { createHash } from "node:crypto";
import type {
  NativeReference,
  T3WorkstreamBinding,
  Workstream,
  WorkstreamCommand,
  WorkstreamReceipt,
} from "@t3tools/contracts";
import {
  WORKSTREAM_CONTRACT_HEADER_VERSION,
  WORKSTREAM_CONTRACT_MANIFEST_SHA256,
} from "@t3tools/contracts";
import type {
  WorkstreamOperationRequest,
  WorkstreamSetPrimaryRequest,
  WorkstreamCreateRequest,
} from "@t3tools/contracts/jones/workstreamOperations";
import * as Schema from "effect/Schema";
import { WorkstreamCommand as CommandSchema } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { WorkstreamGatewayError } from "../../../workstreams/WorkstreamGateway.ts";
import { t3Source, response } from "../registrationContext/testFixtures.ts";
import { canonicalJson } from "./encoding.ts";
import { Ports } from "./ports.ts";
import * as Operations from "./service.ts";

export const hash = (body: string) => createHash("sha256").update(body).digest("hex");
export const environmentId = "source-t3-synthetic";
export const request: WorkstreamSetPrimaryRequest = {
  contract: "workstream-operations/1.0.0",
  operation_id: "operation-test-00001",
  origin: { kind: "app" },
  kind: "set_primary",
  thread: { environment_id: environmentId, thread_id: "thread" },
  target: {
    kind: "workstream",
    registry_id: "registry-test",
    owner_id: "owner-synthetic",
    workstream_id: "target",
  },
  expected: { kind: "unassigned" },
};
export const createRequest: WorkstreamCreateRequest = {
  contract: "workstream-operations/1.0.0",
  operation_id: "operation-create-001",
  origin: { kind: "app" },
  kind: "create_workstream",
  registry: { registry_id: "registry-test", owner_id: "owner-synthetic" },
  workstream: {
    name: "Created",
    lifecycle: "active",
    progress: { state: "progressing" },
    sort_order: 0,
  },
};
export const childId = (request: WorkstreamOperationRequest, kind: string, n = 1) =>
  `jwo-${hash(`jwo/1\0${environmentId}\0${request.operation_id}\0${hash(canonicalJson(request))}`).slice(0, 40)}-${kind}-${n}`;
const at = "1970-01-01T00:00:00.000Z";
const group = (id: string): Workstream => ({
  workstream_id: id,
  owner_id: "owner-synthetic",
  name: id,
  lifecycle: "active",
  progress: { state: "progressing" },
  delivery: "none-observed",
  freshness: "current",
  sort_order: 0,
  version: 1,
  created_at: at,
  updated_at: at,
  created_by: { principal_id: "principal-synthetic" },
  updated_by: { principal_id: "principal-synthetic" },
});
export const offline = () =>
  new WorkstreamGatewayError({ reason: "unknown-effect", detail: "scripted unknown response" });

/** Scripted CP: atomic replay precedes version/business decisions; reads are independent snapshots. */
export const fakeControlPlane = () => {
  const state = {
    availability: null as
      | null
      | "descriptor_absent"
      | "build_unavailable"
      | "contract_mismatch"
      | "source_mismatch"
      | "authority_mismatch"
      | "generation_mismatch"
      | "authority_fenced"
      | "authority_unavailable",
    version: 1,
    reads: 0,
    writes: [] as string[],
    membershipCommits: 0,
    creates: 0,
    expiresAt: "2099-01-01T00:00:00.000Z",
    primary: null as string | null,
    registered: false,
    verified: true,
    writable: true,
    exists: true,
    offline: false,
    verificationPending: false,
    pages: 1,
    receipts: new Map<string, { body: string; receipt: WorkstreamReceipt }>(),
    groups: new Map(["target", "other"].map((id) => [id, group(id)])),
    onSleep: undefined as undefined | ((ms: number) => Effect.Effect<void>),
    beforeRead: undefined as undefined | ((id: string) => void),
    beforeSession: undefined as undefined | (() => void),
    beforeSend: undefined as
      | undefined
      | ((command: WorkstreamCommand, body: string) => Effect.Effect<void, WorkstreamGatewayError>),
    afterSend: undefined as
      | undefined
      | ((command: WorkstreamCommand) => Effect.Effect<void, WorkstreamGatewayError>),
  };
  const context = () => ({
    owner_id: "owner-synthetic",
    server_generation: 5,
    registry_version: state.version,
  });
  const binding = (): T3WorkstreamBinding => ({
    registryId: "https://registry.invalid",
    ownerId: "owner-synthetic",
    principalId: "principal-synthetic",
    authorizationRevision: 3,
    serverGeneration: 5,
    registryVersion: state.version,
    permissions: state.writable ? ["workstreams:read", "workstreams:write"] : ["workstreams:read"],
    contractVersion: WORKSTREAM_CONTRACT_HEADER_VERSION,
    contractManifest: WORKSTREAM_CONTRACT_MANIFEST_SHA256,
  });
  const reference = (): NativeReference => ({
    native_reference_id: "reference",
    owner_id: "owner-synthetic",
    identity: {
      provider: "t3",
      resource_kind: "thread",
      id_kind: "internal",
      source_instance_id: environmentId,
      native_id: "thread",
      account_provenance: { kind: "not_account_scoped" },
    },
    pr_locator: null,
    registration: state.verified
      ? {
          state: "attested",
          attestation_version: 1,
          attested_at: at,
          expires_at: state.expiresAt,
          evidence: {
            provider: "t3",
            source_instance_id: environmentId,
            native_id: "thread",
            authority_namespace: t3Source.authority_namespace,
            store_generation: t3Source.store_generation,
            evidence_sha256: "a".repeat(64),
          },
        }
      : {
          state: "quarantined",
          attestation_version: 0,
          attested_at: null,
          expires_at: null,
          evidence: null,
        },
    created_at: at,
    created_by: { principal_id: "principal-synthetic" },
    created_registry_version: 1,
  });
  const decide = (command: WorkstreamCommand, body = canonicalJson(command)): WorkstreamReceipt => {
    const base = {
      command_id: command.command_id,
      owner_id: "owner-synthetic",
      actor: { principal_id: "principal-synthetic" },
      operation: command.action.operation,
      request_sha256: hash(body),
      server_generation: 5,
      accepted_at: at,
    };
    const reject = (code: "version_conflict" | "membership_conflict") => ({
      ...base,
      state: "rejected" as const,
      completed_at: at,
      error: { code, status: 409, request_id: "error", recovery: "reload" as const },
    });
    let receipt: WorkstreamReceipt;
    if (command.expected_registry_version !== state.version) receipt = reject("version_conflict");
    else if (
      (command.action.operation === "attach_primary" ||
        command.action.operation === "reattach_primary") &&
      state.primary !== null
    )
      receipt = reject("membership_conflict");
    else if (command.action.operation === "verify_reference" && state.verificationPending)
      receipt = { ...base, state: "pending", retry_after_seconds: 1 };
    else {
      const action = command.action;
      let workstreamId: string | null = null;
      let membership = false;
      if (action.operation === "register_reference") {
        if (!state.registered) state.version++;
        state.registered = true;
      } else if (action.operation === "verify_reference") {
        state.verified = true;
        state.version++;
      } else if (action.operation === "create_workstream") {
        workstreamId = `created-${++state.creates}`;
        state.groups.set(workstreamId, group(workstreamId));
        state.version++;
      } else if (action.operation === "attach_primary" || action.operation === "reattach_primary") {
        state.primary = action.workstream_id;
        workstreamId = state.primary;
        membership = true;
      } else if (action.operation === "move_primary") {
        state.primary = action.destination_workstream_id;
        workstreamId = state.primary;
        membership = true;
      } else if (action.operation === "remove_membership") {
        state.primary = null;
        membership = true;
      }
      if (membership) {
        state.membershipCommits++;
        state.version++;
      }
      receipt = {
        ...base,
        state: "committed",
        changed: true,
        completed_at: at,
        registry_version: state.version,
        effects: {
          workstream_versions: workstreamId ? [{ workstream_id: workstreamId, version: 1 }] : [],
          native_reference_id:
            action.operation === "register_reference" || action.operation === "verify_reference"
              ? "reference"
              : null,
          membership_ids: membership ? ["membership"] : [],
          declaration_id: null,
          declaration_revision: null,
          edge_id: null,
          observation: null,
          registration: null,
          lifecycle_declaration: null,
          coordination_disposition: null,
          native_settlement: null,
        },
      };
    }
    state.receipts.set(command.command_id, { body, receipt });
    return receipt;
  };
  const gateway: Ports["Service"]["gateway"] = {
    readSession: () =>
      Effect.suspend(() => {
        state.reads++;
        state.beforeSession?.();
        return state.offline ? Effect.fail(offline()) : Effect.succeed(binding());
      }),
    readReceipt: (id) =>
      Effect.suspend(() => {
        state.reads++;
        const witness_version = state.version;
        state.beforeRead?.(id);
        if (state.offline) return Effect.fail(offline());
        const stored = state.receipts.get(id);
        return Effect.succeed(
          stored
            ? { kind: "receipt" as const, witness_version, receipt: stored.receipt }
            : { kind: "absent" as const, witness_version },
        );
      }),
    resubmit: (_id, body) =>
      Effect.gen(function* () {
        const command = yield* Schema.decodeEffect(Schema.fromJsonString(CommandSchema))(body).pipe(
          Effect.mapError(() => offline()),
        );
        state.writes.push(body);
        if (state.beforeSend) yield* state.beforeSend(command, body);
        if (state.offline) return yield* offline();
        const prior = state.receipts.get(command.command_id);
        if (prior && prior.body !== body) return { kind: "idempotency_conflict" as const };
        const receipt = prior?.receipt ?? decide(command, body);
        if (state.afterSend) yield* state.afterSend(command);
        return { kind: "receipt" as const, receipt };
      }),
    readReference: () =>
      Effect.sync(() => ({ context: context(), reference: reference(), latest_observation: null })),
    readDetail: (id) =>
      Effect.suspend(() =>
        state.groups.has(id)
          ? Effect.succeed({ context: context(), workstream: state.groups.get(id)! })
          : Effect.fail(offline()),
      ),
    readMetadata: () =>
      Effect.succeed({
        binding: binding(),
        items: [...state.groups.values()].map((g) => ({
          workstreamId: g.workstream_id,
          name: g.name,
          lifecycle: g.lifecycle,
          progress: g.progress,
          delivery: g.delivery,
          freshness: g.freshness,
          sortOrder: g.sort_order,
          version: g.version,
          updatedAt: g.updated_at,
        })),
        nextCursor: null,
        source: "live",
        stale: false,
      }),
    readMemberships: (_id, input) =>
      Effect.succeed({
        context: context(),
        items: [],
        next_cursor:
          Number(input?.cursor ?? 0) + 1 < state.pages
            ? String(Number(input?.cursor ?? 0) + 1)
            : null,
      }),
    readThreadPlacements: () =>
      Effect.sync(() => ({
        readiness: "ready" as const,
        trustedEnvironments: [
          {
            environmentId,
            authorityNamespace: t3Source.authority_namespace,
            storeGeneration: t3Source.store_generation,
          },
        ],
        page: {
          context: { ...context(), principal_id: "principal-synthetic", authorization_revision: 3 },
          inventory_sha256: "a".repeat(64),
          next_cursor: null,
          items:
            state.primary && state.verified
              ? [
                  {
                    native_reference_id: "reference",
                    membership_id: "membership",
                    workstream_id: state.primary,
                    kind: "primary" as const,
                    source_instance_id: environmentId,
                    native_thread_id: "thread",
                    attestation_version: 1,
                    attested_at: at,
                    expires_at: state.expiresAt,
                    authority_namespace: t3Source.authority_namespace,
                    store_generation: t3Source.store_generation,
                    evidence_sha256: "a".repeat(64),
                    source_binding_version: 1,
                  },
                ]
              : [],
        },
      })),
  };
  const ports: Ports["Service"] = {
    environmentId,
    registryId: "registry-test",
    activation: { state: "enabled", missing_keys: [] },
    gateway,
    registration: {
      read: () =>
        Effect.succeed({
          context: { ...response, ...context() },
          t3Availability:
            state.availability === null
              ? { state: "ready" }
              : { state: "unavailable", reason: state.availability },
        }),
    },
    threadExists: () => Effect.succeed(state.exists),
    now: Clock.currentTimeMillis,
    sleep: (ms) =>
      Effect.gen(function* () {
        if (state.onSleep) yield* state.onSleep(ms);
        yield* Effect.sleep(ms);
      }),
    jitter: Effect.succeed(0),
  };
  const layer = Operations.layer.pipe(
    Layer.provide(Layer.succeed(Ports, ports)),
    Layer.provide(NodeCrypto.layer),
  );
  const seedRegistration = (operation = request) =>
    decide({
      command_id: childId(operation, "reg"),
      expected_registry_version: state.version,
      expected_server_generation: 5,
      action: { operation: "register_reference", identity: reference().identity, pr_locator: null },
    });
  return { state, ports, layer, decide, seedRegistration, binding };
};
