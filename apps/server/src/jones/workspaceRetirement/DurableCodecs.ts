import * as Schema from "effect/Schema";
import { CONSUMER_LIMITS } from "./ConsumerProbe.ts";
import type { DutyIdentity } from "./DutyIdentity.ts";
import type { NativeDutyOwner } from "./NativeDutyOwner.ts";
import type { RetirementReceipt } from "./Receipts.ts";

const strings = Schema.mutable(Schema.Array(Schema.String));
const fields = Schema.Record(Schema.String, Schema.String);
const binding = Schema.Struct({
  path: Schema.String,
  dev: Schema.Number,
  ino: Schema.Number,
  git_dir: Schema.String,
  git_dir_dev: Schema.Number,
  git_dir_ino: Schema.Number,
  git_common_dir: Schema.String,
  admin_id: Schema.String,
  breadcrumb_id: Schema.NullOr(Schema.String),
});
const host = Schema.Struct({
  host: Schema.String,
  boot_id: Schema.String,
  pid: Schema.Number,
  process_start: Schema.String,
  process_namespace: Schema.String,
});
export const DutyIdentityJson = Schema.fromJsonString(
  Schema.Union([
    Schema.Struct({
      status: Schema.Literal("bound"),
      incarnation: Schema.String,
      binding: Schema.Struct({
        path: Schema.String,
        dev: Schema.String,
        ino: Schema.String,
        gitDirectory: Schema.String,
        gitDevice: Schema.String,
        gitInode: Schema.String,
        commonDirectory: Schema.String,
      }),
    }),
    Schema.Struct({
      status: Schema.Literal("unbound"),
      reason: Schema.Literal("native_worktree_identity_unavailable"),
      nextAction: Schema.String,
    }),
  ]),
) satisfies Schema.Codec<DutyIdentity, string>;
export const NativeDutyOwnerJson = Schema.fromJsonString(
  Schema.Union([
    Schema.Struct({
      schema: Schema.Literal("workspace-duty-owner/v1"),
      type: Schema.Literal("in-process"),
      host,
      nonce: Schema.String,
    }),
    Schema.Struct({
      schema: Schema.Literal("workspace-duty-owner/v1"),
      type: Schema.Literal("native-session"),
      host,
      pid: Schema.Number,
      start: Schema.String,
      session: Schema.String,
    }),
    Schema.Struct({
      schema: Schema.Literal("workspace-duty-owner/v1"),
      type: Schema.Literal("unresolved"),
      reason: Schema.String,
      nextAction: Schema.String,
    }),
  ]),
) satisfies Schema.Codec<NativeDutyOwner, string>;
export const DutyHolderJson = Schema.fromJsonString(
  Schema.Struct({
    serverPid: Schema.Number,
    nonce: Schema.String,
    kind: Schema.String,
    owner: Schema.String,
  }),
);
export const QueuedHolderJson = Schema.fromJsonString(
  Schema.Struct({ runId: Schema.String, threadId: Schema.String }),
);
const detail = Schema.Struct({
  reason: Schema.String,
  target: Schema.String,
  root: Schema.String,
  category: Schema.Literals([
    "active_use", "unique_work", "missing_proof", "unavailable_observation", "protection",
  ]),
  fact: Schema.String,
  missingFact: Schema.NullOr(Schema.String),
  lineage: Schema.Array(fields),
  nextAction: Schema.String,
});
const qualification = Schema.Struct({
  schema: Schema.Literal("workspace-qualification/v1"),
  authority_effect: Schema.Literal("none"),
  outcome: Schema.Literals(["retirable", "hold", "unknown"]),
  level: Schema.NullOr(Schema.Literals(["contained", "branch-retained"])),
  reasons: strings,
  decision: Schema.Literals(["propose", "hold", "unknown"]),
  observationMs: Schema.Number,
  binding: Schema.optional(binding),
  head: Schema.optional(Schema.String),
  branch: Schema.optional(Schema.NullOr(Schema.String)),
  containingRefs: Schema.optional(strings),
  preservationTips: Schema.optional(strings),
  untrackedPaths: Schema.optional(strings),
  ignored: Schema.optional(
    Schema.mutable(Schema.Array(Schema.Struct({
      path: Schema.String,
      outcome: Schema.Literals(["disposable", "hold", "unknown"]),
      reason: Schema.NullOr(Schema.String),
      basis: Schema.optional(Schema.String),
      manifestSha256: Schema.optional(Schema.String),
      paths: Schema.optional(Schema.Array(Schema.String)),
      count: Schema.optional(Schema.Number),
    }))),
  ),
  consumers: Schema.optional(Schema.Struct({
    holds: strings,
    unknown: strings,
    coverage: Schema.Record(Schema.String, Schema.Struct({
      status: Schema.String,
      ageMs: Schema.Number,
      limits: Schema.Array(Schema.String),
      reason: Schema.NullOr(Schema.String),
    })),
    limits: Schema.Tuple([
      Schema.Literal(CONSUMER_LIMITS[0]),
      Schema.Literal(CONSUMER_LIMITS[1]),
      Schema.Literal(CONSUMER_LIMITS[2]),
      Schema.Literal(CONSUMER_LIMITS[3]),
      Schema.Literal(CONSUMER_LIMITS[4]),
    ]),
  })),
  capturedConfig: Schema.optional(fields),
  sparsePatterns: Schema.optional(Schema.String),
  fingerprint: Schema.optional(Schema.String),
  details: Schema.optional(Schema.mutable(Schema.Array(detail))),
  lineageObservation: Schema.optional(Schema.Struct({
    status: Schema.Literals(["available", "partial", "unavailable"]),
    lineage: Schema.mutable(Schema.Array(fields)),
  })),
});
export const RetirementReceiptJson = Schema.fromJsonString(
  Schema.Struct({
    schema: Schema.Literal("worktree-retirement/v1"),
    id: Schema.String,
    cause: Schema.Literals(["settlement", "sweep", "deletion", "owner_action"]),
    state: Schema.Literals(["intent", "retired", "not_retired", "partial", "unknown"]),
    binding,
    incarnation: Schema.String,
    qualification,
    coverage: Schema.Unknown,
    preservationRefs: Schema.mutable(Schema.Array(Schema.Struct({
      ref: Schema.String,
      head: Schema.String,
    }))),
    journal: Schema.String,
    disposed: strings,
    reasons: strings,
    limits: Schema.Array(Schema.String),
    removalDiagnostic: Schema.optional(Schema.String),
    bytes: Schema.Struct({
      logical: Schema.NullOr(Schema.Number),
      allocated: Schema.NullOr(Schema.Number),
      statvfs_delta: Schema.NullOr(Schema.Number),
    }),
  }),
) satisfies Schema.Codec<RetirementReceipt, string>;
