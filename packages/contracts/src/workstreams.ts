import * as Schema from "effect/Schema";

export const WORKSTREAM_CONTRACT_FAMILY = "workstreams" as const;
export const WORKSTREAM_CONTRACT_VERSION = "1.0.0" as const;
export const WORKSTREAM_CONTRACT_HEADER_VERSION = "workstreams/1.0.0" as const;
export const WORKSTREAM_CONTRACT_MANIFEST_SHA256 =
  "6d8da23d51c1bba024dddc4b8d4dd9a594d2f474affd73e4cc7de508b797f557" as const;
export const WORKSTREAM_MAX_PAGE_ITEMS = 100;
export const WORKSTREAM_MAX_RESPONSE_BYTES = 1_048_576;

const Id = Schema.String.check(
  Schema.isMaxLength(128),
  Schema.isPattern(/^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,127}$/),
);
const CommandId = Schema.String.check(
  Schema.isMinLength(16),
  Schema.isMaxLength(128),
  Schema.isPattern(/^[A-Za-z0-9][A-Za-z0-9._:-]{15,127}$/),
);
const TIMESTAMP_PATTERN = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d{1,6})?Z$/;
const Timestamp = Schema.String.check(
  Schema.isMaxLength(27),
  Schema.isPattern(TIMESTAMP_PATTERN),
  Schema.makeFilter((value) => {
    const match = TIMESTAMP_PATTERN.exec(value);
    if (match === null) return false;
    const [, yearText, monthText, dayText, hourText, minuteText, secondText] = match;
    const year = Number(yearText);
    const month = Number(monthText);
    const day = Number(dayText);
    const hour = Number(hourText);
    const minute = Number(minuteText);
    const second = Number(secondText);
    const leapYear = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
    const daysInMonth = [31, leapYear ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    return (
      month >= 1 &&
      month <= 12 &&
      day >= 1 &&
      day <= (daysInMonth[month - 1] ?? 0) &&
      hour <= 23 &&
      minute <= 59 &&
      second <= 59
    );
  }),
);
const Version = Schema.Number.check(
  Schema.isInt(),
  Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER }),
);
const PositiveVersion = Schema.Number.check(
  Schema.isInt(),
  Schema.isBetween({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER }),
);
const Name = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(160),
  Schema.isPattern(/\S/),
);
const DeclarationText = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(4096),
  Schema.isPattern(/\S/),
);
const NativeId = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(512),
  Schema.isPattern(/^[^\u0000-\u001f\u007f]+$/),
);
const Cursor = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(512),
  Schema.isPattern(/^[A-Za-z0-9_-]{1,512}$/),
);
const Sha256 = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));
const CanonicalHttpsOrigin = Schema.String.check(
  Schema.isMaxLength(512),
  Schema.makeFilter((value) => {
    try {
      const parsed = new URL(value);
      return parsed.protocol === "https:" && parsed.origin === value;
    } catch {
      return false;
    }
  }),
);

export const WorkstreamLifecycle = Schema.Literals([
  "planned",
  "active",
  "paused",
  "completed",
  "deferred",
  "abandoned",
]);
export type WorkstreamLifecycle = typeof WorkstreamLifecycle.Type;

export const WorkstreamProgress = Schema.Union([
  Schema.Struct({ state: Schema.Literals(["progressing", "unknown", "stale"]) }),
  Schema.Struct({ state: Schema.Literal("waiting"), condition: DeclarationText }),
  Schema.Struct({ state: Schema.Literal("blocked"), impediment: DeclarationText }),
]);
export type WorkstreamProgress = typeof WorkstreamProgress.Type;

export const WorkstreamDelivery = Schema.Literals([
  "none-observed",
  "branch-open",
  "pr-open",
  "merged",
  "released",
  "deployed",
  "deployment-verified",
  "failed",
  "unknown",
  "stale",
]);
export type WorkstreamDelivery = typeof WorkstreamDelivery.Type;

export const WorkstreamFreshness = Schema.Literals([
  "current",
  "stale",
  "inaccessible",
  "partial-coverage",
  "conflicting",
  "unknown",
]);
export type WorkstreamFreshness = typeof WorkstreamFreshness.Type;

export const WorkstreamActor = Schema.Struct({ principal_id: Id });
export type WorkstreamActor = typeof WorkstreamActor.Type;

export const WorkstreamReadContext = Schema.Struct({
  owner_id: Id,
  server_generation: PositiveVersion,
  registry_version: Version,
});
export type WorkstreamReadContext = typeof WorkstreamReadContext.Type;

export const Workstream = Schema.Struct({
  workstream_id: Id,
  owner_id: Id,
  name: Name,
  lifecycle: WorkstreamLifecycle,
  progress: WorkstreamProgress,
  delivery: WorkstreamDelivery,
  freshness: WorkstreamFreshness,
  sort_order: Version.check(Schema.isLessThanOrEqualTo(2_147_483_647)),
  version: PositiveVersion,
  created_at: Timestamp,
  updated_at: Timestamp,
  created_by: WorkstreamActor,
  updated_by: WorkstreamActor,
});
export type Workstream = typeof Workstream.Type;

export const AccountProvenance = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("account"),
    account_id: Id,
    account_namespace: NativeId,
  }),
  Schema.Struct({ kind: Schema.Literal("not_account_scoped") }),
]);

export const WorkstreamPrLocator = Schema.Struct({
  host: Schema.Literal("github.com"),
  repository_owner: Schema.String.check(
    Schema.isMaxLength(39),
    Schema.isPattern(/^[a-z0-9](?:[a-z0-9-]{0,37}[a-z0-9])?$/),
  ),
  repository_name: Schema.String.check(
    Schema.isMaxLength(100),
    Schema.isPattern(/^[a-z0-9._-]{1,100}$/),
  ),
  number: PositiveVersion.check(Schema.isLessThanOrEqualTo(2_147_483_647)),
});
export type WorkstreamPrLocator = typeof WorkstreamPrLocator.Type;

export function canonicalGitHubPullRequestUrl(locator: WorkstreamPrLocator): string {
  if (
    !/^[A-Za-z0-9_.-]+$/.test(locator.repository_owner) ||
    !/^[A-Za-z0-9_.-]+$/.test(locator.repository_name) ||
    locator.repository_owner === "." ||
    locator.repository_owner === ".." ||
    locator.repository_name === "." ||
    locator.repository_name === ".." ||
    locator.repository_owner.length > 100 ||
    locator.repository_name.length > 100 ||
    !Number.isSafeInteger(locator.number) ||
    locator.number < 1
  ) {
    throw new Error("Invalid GitHub pull-request locator");
  }
  return `https://github.com/${locator.repository_owner}/${locator.repository_name}/pull/${locator.number}`;
}

export const WorkstreamNativeIdentity = Schema.Struct({
  provider: Schema.String.check(Schema.isPattern(/^[a-z][a-z0-9-]{0,62}$/)),
  source_instance_id: Id,
  resource_kind: Schema.String.check(Schema.isPattern(/^[a-z][a-z0-9_]{0,62}$/)),
  id_kind: Schema.Literals(["internal", "external"]),
  native_id: NativeId,
  account_provenance: AccountProvenance,
});
export const WorkstreamRegistrationVerification = Schema.Struct({
  state: Schema.Literals([
    "quarantined",
    "verification-pending",
    "attested",
    "verification-failed",
    "expired",
    "re-attestation-required",
  ]),
  attestation_version: Version,
  attested_at: Schema.NullOr(Timestamp),
  expires_at: Schema.NullOr(Timestamp),
  evidence: Schema.NullOr(
    Schema.Struct({
      provider: Schema.String.check(Schema.isPattern(/^[a-z][a-z0-9-]{0,62}$/)),
      source_instance_id: Id,
      authority_namespace: NativeId,
      native_id: NativeId,
      store_generation: PositiveVersion,
      evidence_sha256: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
    }),
  ),
}).check(
  Schema.makeFilter(
    (value) =>
      (value.state === "attested"
        ? value.attestation_version > 0 &&
          value.attested_at !== null &&
          value.expires_at !== null &&
          value.evidence !== null
        : value.state === "quarantined"
          ? value.attestation_version === 0 &&
            value.attested_at === null &&
            value.expires_at === null &&
            value.evidence === null
          : true) || "Registration fields do not match the verification state.",
  ),
);
export const NativeReference = Schema.Struct({
  native_reference_id: Id,
  owner_id: Id,
  identity: WorkstreamNativeIdentity,
  pr_locator: Schema.NullOr(WorkstreamPrLocator),
  registration: WorkstreamRegistrationVerification,
  created_at: Timestamp,
  created_by: WorkstreamActor,
  created_registry_version: PositiveVersion,
});
export type NativeReference = typeof NativeReference.Type;

const EpisodeBoundary = Schema.Struct({
  at: Timestamp,
  actor: WorkstreamActor,
  command_id: CommandId,
  registry_version: PositiveVersion,
});
const EpisodeEnd = Schema.Struct({
  ...EpisodeBoundary.fields,
  reason: Schema.Literals([
    "removed",
    "completed",
    "completed-and-delivery-verified",
    "superseded",
    "continued-elsewhere",
    "deferred",
    "abandoned",
    "other",
  ]),
  other_reason: Schema.NullOr(DeclarationText),
}).check(
  Schema.makeFilter(
    (value) =>
      (value.reason === "other" ? value.other_reason !== null : value.other_reason === null) ||
      "Only the other reason accepts explanatory text.",
  ),
);
export const MembershipEpisode = Schema.Struct({
  membership_id: Id,
  workstream_id: Id,
  native_reference_id: Id,
  kind: Schema.Literals(["primary", "secondary"]),
  opened: EpisodeBoundary,
  closed: Schema.NullOr(EpisodeEnd),
});
export type MembershipEpisode = typeof MembershipEpisode.Type;

export const DeclarationRevision = Schema.Struct({
  declaration_id: Id,
  workstream_id: Id,
  revision: PositiveVersion,
  text: DeclarationText,
  state: Schema.Literals(["active", "withdrawn"]),
  recorded_at: Timestamp,
  actor: WorkstreamActor,
  command_id: CommandId,
  registry_version: PositiveVersion,
});
export type DeclarationRevision = typeof DeclarationRevision.Type;

export const WorkstreamEdge = Schema.Struct({
  edge_id: Id,
  from_workstream_id: Id,
  to_workstream_id: Id,
  relation: Schema.Literals(["continues_as", "superseded_by"]),
  opened: EpisodeBoundary,
  closed: Schema.NullOr(EpisodeBoundary),
});
export type WorkstreamEdge = typeof WorkstreamEdge.Type;

export const WorkstreamLifecycleDeclaration = Schema.Struct({
  lifecycle_declaration_id: Id,
  workstream_id: Id,
  revision: PositiveVersion,
  prior_lifecycle: Schema.NullOr(WorkstreamLifecycle),
  new_lifecycle: WorkstreamLifecycle,
  recorded_at: Timestamp,
  actor: WorkstreamActor,
  command_id: CommandId,
  supersedes_lifecycle_declaration_id: Schema.NullOr(Id),
  registry_version: PositiveVersion,
}).check(
  Schema.makeFilter(
    (value) =>
      value.prior_lifecycle === null ||
      !(["completed", "abandoned"] as const).includes(
        value.prior_lifecycle as "completed" | "abandoned",
      ) ||
      value.supersedes_lifecycle_declaration_id !== null ||
      "Reopening a terminal lifecycle requires the superseded declaration.",
  ),
);

export const WorkstreamOperation = Schema.Literals([
  "create_workstream",
  "update_workstream",
  "register_reference",
  "verify_reference",
  "attach_primary",
  "reattach_primary",
  "link_secondary",
  "remove_membership",
  "move_primary",
  "set_coordination_disposition",
  "request_native_t3_settlement",
  "set_declaration",
  "withdraw_declaration",
  "add_edge",
  "remove_edge",
  "refresh_linked_pr",
]);
export type WorkstreamOperation = typeof WorkstreamOperation.Type;

const CreateWorkstreamAction = Schema.Struct({
  operation: Schema.Literal("create_workstream"),
  name: Name,
  lifecycle: WorkstreamLifecycle,
  progress: WorkstreamProgress,
  sort_order: Version.check(Schema.isLessThanOrEqualTo(2_147_483_647)),
});
const UpdateWorkstreamAction = Schema.Struct({
  operation: Schema.Literal("update_workstream"),
  workstream_id: Id,
  expected_version: PositiveVersion,
  name: Name,
  lifecycle: WorkstreamLifecycle,
  progress: WorkstreamProgress,
  sort_order: Version.check(Schema.isLessThanOrEqualTo(2_147_483_647)),
});
const RegisterReferenceAction = Schema.Struct({
  operation: Schema.Literal("register_reference"),
  identity: NativeReference.fields.identity,
  pr_locator: Schema.NullOr(WorkstreamPrLocator),
});
const VerifyReferenceAction = Schema.Struct({
  operation: Schema.Literal("verify_reference"),
  native_reference_id: Id,
  expected_attestation_version: Version,
});
const AttachPrimaryAction = Schema.Struct({
  operation: Schema.Literals(["attach_primary", "reattach_primary"]),
  workstream_id: Id,
  expected_version: PositiveVersion,
  native_reference_id: Id,
});
const LinkSecondaryAction = Schema.Struct({
  operation: Schema.Literal("link_secondary"),
  workstream_id: Id,
  expected_version: PositiveVersion,
  native_reference_id: Id,
});
const RemoveMembershipAction = Schema.Struct({
  operation: Schema.Literal("remove_membership"),
  workstream_id: Id,
  expected_version: PositiveVersion,
  membership_id: Id,
});
const MovePrimaryAction = Schema.Struct({
  operation: Schema.Literal("move_primary"),
  source_workstream_id: Id,
  expected_source_version: PositiveVersion,
  source_membership_id: Id,
  destination_workstream_id: Id,
  expected_destination_version: PositiveVersion,
});
const SetDispositionAction = Schema.Struct({
  operation: Schema.Literal("set_coordination_disposition"),
  workstream_id: Id,
  expected_version: PositiveVersion,
  membership_id: Id,
  disposition: Schema.Literals([
    "completed",
    "completed-and-delivery-verified",
    "superseded",
    "continued-elsewhere",
    "deferred",
    "abandoned",
    "other",
  ]),
  other_disposition: Schema.NullOr(DeclarationText),
}).check(
  Schema.makeFilter(
    (value) =>
      (value.disposition === "other"
        ? value.other_disposition !== null
        : value.other_disposition === null) ||
      "Only the other disposition accepts explanatory text.",
  ),
);
const NativeSettlementAction = Schema.Struct({
  operation: Schema.Literal("request_native_t3_settlement"),
  native_reference_id: Id,
  expected_attestation_version: PositiveVersion,
  native_action: Schema.Literals(["settle", "unsettle"]),
});
const SetDeclarationAction = Schema.Struct({
  operation: Schema.Literal("set_declaration"),
  workstream_id: Id,
  expected_version: PositiveVersion,
  declaration_id: Schema.NullOr(Id),
  expected_revision: Version,
  text: DeclarationText,
});
const WithdrawDeclarationAction = Schema.Struct({
  operation: Schema.Literal("withdraw_declaration"),
  workstream_id: Id,
  expected_version: PositiveVersion,
  declaration_id: Id,
  expected_revision: PositiveVersion,
});
const AddEdgeAction = Schema.Struct({
  operation: Schema.Literal("add_edge"),
  from_workstream_id: Id,
  expected_from_version: PositiveVersion,
  to_workstream_id: Id,
  expected_to_version: PositiveVersion,
  relation: Schema.Literals(["continues_as", "superseded_by"]),
});
const RemoveEdgeAction = Schema.Struct({
  operation: Schema.Literal("remove_edge"),
  edge_id: Id,
  from_workstream_id: Id,
  expected_from_version: PositiveVersion,
  to_workstream_id: Id,
  expected_to_version: PositiveVersion,
});
const RefreshLinkedPrAction = Schema.Struct({
  operation: Schema.Literal("refresh_linked_pr"),
  workstream_id: Id,
  expected_version: PositiveVersion,
  membership_id: Id,
  expected_observation_version: Version,
});
export const WorkstreamAction = Schema.Union([
  CreateWorkstreamAction,
  UpdateWorkstreamAction,
  RegisterReferenceAction,
  VerifyReferenceAction,
  AttachPrimaryAction,
  LinkSecondaryAction,
  RemoveMembershipAction,
  MovePrimaryAction,
  SetDispositionAction,
  NativeSettlementAction,
  SetDeclarationAction,
  WithdrawDeclarationAction,
  AddEdgeAction,
  RemoveEdgeAction,
  RefreshLinkedPrAction,
]);
export const WorkstreamCommand = Schema.Struct({
  command_id: CommandId,
  expected_server_generation: PositiveVersion,
  expected_registry_version: Version,
  action: WorkstreamAction,
});
export type WorkstreamCommand = typeof WorkstreamCommand.Type;

const ReceiptBase = {
  command_id: CommandId,
  owner_id: Id,
  actor: WorkstreamActor,
  operation: WorkstreamOperation,
  request_sha256: Sha256,
  server_generation: PositiveVersion,
  accepted_at: Timestamp,
};
const errorBindings = {
  unauthorized: [401, "stop"],
  forbidden: [403, "stop"],
  not_found: [404, "stop"],
  invalid_request: [400, "correct_request"],
  contract_mismatch: [409, "stop"],
  version_conflict: [409, "reload"],
  registration_unverified: [409, "reverify"],
  generation_conflict: [409, "reconcile"],
  identity_conflict: [409, "correct_request"],
  membership_conflict: [409, "reload"],
  relationship_conflict: [409, "correct_request"],
  cycle_conflict: [409, "correct_request"],
  idempotency_conflict: [409, "reconcile"],
  cursor_invalid: [400, "restart_page"],
  cursor_stale: [409, "restart_page"],
  payload_too_large: [413, "correct_request"],
  unsupported_media_type: [415, "correct_request"],
  backpressure: [429, "retry_after"],
  provider_unconfigured: [503, "stop"],
  unavailable: [503, "reconcile"],
  unknown_effect: [503, "reconcile"],
} as const;
export const WorkstreamError = Schema.Struct({
  code: Schema.Literals(
    Object.keys(errorBindings) as [keyof typeof errorBindings, ...(keyof typeof errorBindings)[]],
  ),
  status: Schema.Number.check(Schema.isInt(), Schema.isBetween({ minimum: 400, maximum: 599 })),
  request_id: Id,
  recovery: Schema.Literals([
    "stop",
    "correct_request",
    "reload",
    "reconcile",
    "restart_page",
    "retry_after",
    "reverify",
  ]),
  retry_after_seconds: Schema.optionalKey(PositiveVersion.check(Schema.isLessThanOrEqualTo(3600))),
}).check(
  Schema.makeFilter((value) => {
    const [status, recovery] = errorBindings[value.code];
    return (
      (value.status === status &&
        value.recovery === recovery &&
        (value.code === "backpressure"
          ? value.retry_after_seconds !== undefined
          : value.retry_after_seconds === undefined)) ||
      "Error status, recovery, and retry fields must match its code."
    );
  }),
);

const WorkstreamPrSnapshot = Schema.Struct({
  state: Schema.Literals(["open", "closed", "merged"]),
  draft: Schema.Boolean,
  observed_at: Timestamp,
  provider_updated_at: Schema.NullOr(Timestamp),
});
export const WorkstreamPrObservation = Schema.Struct({
  native_reference_id: Id,
  observation_version: PositiveVersion,
  attempted_at: Timestamp,
  outcome: Schema.Literals([
    "observed",
    "not_modified",
    "inaccessible",
    "rate_limited",
    "unavailable",
    "invalid_response",
  ]),
  retry_after_seconds: Schema.NullOr(PositiveVersion.check(Schema.isLessThanOrEqualTo(3600))),
  last_success: Schema.NullOr(WorkstreamPrSnapshot),
  command_id: CommandId,
});

export const WorkstreamResourceVersion = Schema.Struct({
  workstream_id: Id,
  version: PositiveVersion,
});
export const WorkstreamReceiptEffects = Schema.Struct({
  workstream_versions: Schema.Array(WorkstreamResourceVersion).check(Schema.isMaxLength(2)),
  native_reference_id: Schema.NullOr(Id),
  membership_ids: Schema.Array(Id).check(Schema.isMaxLength(2)),
  declaration_id: Schema.NullOr(Id),
  declaration_revision: Schema.NullOr(PositiveVersion),
  edge_id: Schema.NullOr(Id),
  observation: Schema.NullOr(WorkstreamPrObservation),
  registration: Schema.NullOr(WorkstreamRegistrationVerification),
  lifecycle_declaration: Schema.NullOr(WorkstreamLifecycleDeclaration),
  coordination_disposition: Schema.NullOr(
    Schema.Struct({
      membership_id: Id,
      disposition: Schema.Literals([
        "completed",
        "completed-and-delivery-verified",
        "superseded",
        "continued-elsewhere",
        "deferred",
        "abandoned",
        "other",
      ]),
    }),
  ),
  native_settlement: Schema.NullOr(
    Schema.Struct({
      native_reference_id: Id,
      native_action: Schema.Literals(["settle", "unsettle"]),
      outcome: Schema.Literals(["committed", "denied", "unsupported", "failed", "unresolved"]),
    }),
  ),
});
export const WorkstreamReceipt = Schema.Union([
  Schema.Struct({
    ...ReceiptBase,
    state: Schema.Literal("committed"),
    completed_at: Timestamp,
    registry_version: Version,
    changed: Schema.Boolean,
    effects: WorkstreamReceiptEffects,
  }),
  Schema.Struct({
    ...ReceiptBase,
    state: Schema.Literals(["pending", "unresolved"]),
    retry_after_seconds: PositiveVersion.check(Schema.isLessThanOrEqualTo(3600)),
  }),
  Schema.Struct({
    ...ReceiptBase,
    state: Schema.Literal("rejected"),
    completed_at: Timestamp,
    error: WorkstreamError,
  }),
]);
export type WorkstreamReceipt = typeof WorkstreamReceipt.Type;
export const WorkstreamCommittedReceipt = WorkstreamReceipt.members[0];
export const WorkstreamPendingReceipt = WorkstreamReceipt.members[1];
export const WorkstreamRejectedReceipt = WorkstreamReceipt.members[2];
export const WorkstreamTerminalReceipt = Schema.Union([
  WorkstreamCommittedReceipt,
  WorkstreamRejectedReceipt,
]);

const WorkstreamPermission = Schema.Literals(["workstreams:read", "workstreams:write"]);
const UniqueWorkstreamPermissions = Schema.Array(WorkstreamPermission).check(
  Schema.isMaxLength(2),
  Schema.makeFilter(
    (value) => new Set(value).size === value.length || "Permissions must be unique.",
  ),
);
const NonEmptyWorkstreamPermissions = UniqueWorkstreamPermissions.check(Schema.isMinLength(1));

export const WorkstreamOwnerGrant = Schema.Struct({
  owner_id: Id,
  principal_id: Id,
  grant_id: Id,
  grant_version: PositiveVersion,
  permissions: NonEmptyWorkstreamPermissions,
  state: Schema.Literals(["active", "revoked"]),
});

export const WorkstreamCapabilities = Schema.Struct({
  contract_family: Schema.Literal(WORKSTREAM_CONTRACT_FAMILY),
  contract_version: Schema.Literal(WORKSTREAM_CONTRACT_VERSION),
  manifest_sha256: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
  context: WorkstreamReadContext,
  permissions: UniqueWorkstreamPermissions,
  max_request_bytes: Schema.Literal(32_768),
  max_response_bytes: Schema.Literal(1_048_576),
  max_json_depth: Schema.Literal(10),
  max_page_items: Schema.Literal(100),
  max_pr_response_bytes: Schema.Literal(262_144),
  max_pr_request_seconds: Schema.Literal(15),
  cursor_ttl_seconds: Schema.Literal(900),
});
export type WorkstreamCapabilities = typeof WorkstreamCapabilities.Type;

const page = <Item extends Schema.Top>(item: Item) =>
  Schema.Struct({
    context: WorkstreamReadContext,
    items: Schema.Array(item).check(Schema.isMaxLength(WORKSTREAM_MAX_PAGE_ITEMS)),
    next_cursor: Schema.NullOr(Cursor),
  });
export const WorkstreamPage = page(Workstream);
export const WorkstreamReferencePage = page(NativeReference);
export const WorkstreamMembershipPage = page(MembershipEpisode);
export const WorkstreamDeclarationPage = page(DeclarationRevision);
export const WorkstreamEdgePage = page(WorkstreamEdge);
export const WorkstreamAuditEvent = Schema.Struct({
  event_id: Id,
  command_id: CommandId,
  actor: WorkstreamActor,
  operation: WorkstreamOperation,
  occurred_at: Timestamp,
  registry_version: Version,
  changed: Schema.Boolean,
  workstream_versions: Schema.Array(WorkstreamResourceVersion).check(Schema.isMaxLength(2)),
  native_reference_id: Schema.NullOr(Id),
  membership_ids: Schema.Array(Id).check(Schema.isMaxLength(2)),
  declaration_id: Schema.NullOr(Id),
  declaration_revision: Schema.NullOr(PositiveVersion),
  edge_id: Schema.NullOr(Id),
  lifecycle_declaration: Schema.NullOr(WorkstreamLifecycleDeclaration),
});
export const WorkstreamHistoryPage = page(WorkstreamAuditEvent);
export type WorkstreamPage = typeof WorkstreamPage.Type;
export type WorkstreamReferencePage = typeof WorkstreamReferencePage.Type;
export type WorkstreamMembershipPage = typeof WorkstreamMembershipPage.Type;
export type WorkstreamDeclarationPage = typeof WorkstreamDeclarationPage.Type;
export type WorkstreamEdgePage = typeof WorkstreamEdgePage.Type;
export type WorkstreamHistoryPage = typeof WorkstreamHistoryPage.Type;

export const WorkstreamDetail = Schema.Struct({
  context: WorkstreamReadContext,
  workstream: Workstream,
});
export type WorkstreamDetail = typeof WorkstreamDetail.Type;
export const WorkstreamReferenceDetail = Schema.Struct({
  context: WorkstreamReadContext,
  reference: NativeReference,
  latest_observation: Schema.NullOr(WorkstreamPrObservation),
});
export type WorkstreamReferenceDetail = typeof WorkstreamReferenceDetail.Type;

export const WorkstreamSession = Schema.Struct({
  context: WorkstreamReadContext,
  permissions: UniqueWorkstreamPermissions,
  csrf_token: Schema.String.check(
    Schema.isMaxLength(96),
    Schema.isPattern(/^v1\.[0-9]{1,16}\.[A-Za-z0-9_-]{32}\.[A-Za-z0-9_-]{43}$/),
  ),
  csrf_ttl_seconds: Schema.Literal(900),
});

export const T3WorkstreamBinding = Schema.Struct({
  registryId: CanonicalHttpsOrigin,
  ownerId: Id,
  principalId: Id,
  authorizationRevision: PositiveVersion,
  serverGeneration: PositiveVersion,
  registryVersion: Version,
  permissions: UniqueWorkstreamPermissions,
  contractVersion: Schema.Literal(WORKSTREAM_CONTRACT_HEADER_VERSION),
  contractManifest: Schema.Literal(WORKSTREAM_CONTRACT_MANIFEST_SHA256),
});
export type T3WorkstreamBinding = typeof T3WorkstreamBinding.Type;

export const T3WorkstreamMetadata = Schema.Struct({
  workstreamId: Id,
  name: Name,
  lifecycle: WorkstreamLifecycle,
  progress: WorkstreamProgress,
  delivery: WorkstreamDelivery,
  freshness: WorkstreamFreshness,
  sortOrder: Version.check(Schema.isLessThanOrEqualTo(2_147_483_647)),
  version: PositiveVersion,
  updatedAt: Timestamp,
});
export type T3WorkstreamMetadata = typeof T3WorkstreamMetadata.Type;

export const T3WorkstreamListResult = Schema.Struct({
  binding: T3WorkstreamBinding,
  items: Schema.Array(T3WorkstreamMetadata).check(Schema.isMaxLength(WORKSTREAM_MAX_PAGE_ITEMS)),
  nextCursor: Schema.NullOr(Cursor),
  source: Schema.Literals(["live", "cache"]),
  stale: Schema.Boolean,
});
export type T3WorkstreamListResult = typeof T3WorkstreamListResult.Type;

export const T3WorkstreamListQuery = {
  limit: Schema.optional(
    Schema.FiniteFromString.check(Schema.isInt(), Schema.isBetween({ minimum: 1, maximum: 100 })),
  ),
  cursor: Schema.optional(Cursor),
};

export const T3WorkstreamCommandRequest = Schema.Struct({ command: WorkstreamCommand });
export const T3WorkstreamCommandPollParams = Schema.Struct({ commandId: CommandId });
export const T3WorkstreamDetailParams = Schema.Struct({ workstreamId: Id });
export const T3WorkstreamReferenceParams = Schema.Struct({ nativeReferenceId: Id });
export const T3WorkstreamPageQuery = T3WorkstreamListQuery;
