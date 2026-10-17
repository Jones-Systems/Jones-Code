import type { MembershipEpisode, NativeReference } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { buildWorkstreamAssignmentIndex } from "./assignmentIndex.ts";
import {
  groupNativeThreadsByWorkstream,
  nativeWorkstreamThreadKey,
  secondaryNativeWorkstreamLabels,
} from "./nativeThreadGrouping.ts";

const trustedNow = "2026-09-12T12:00:00Z";
const trustedEnvironments = (...environmentIds: readonly string[]) =>
  new Map(
    environmentIds.map(
      (environmentId) =>
        [environmentId, { authorityNamespace: environmentId, storeGeneration: 1 }] as const,
    ),
  );

const ws = (workstreamId: string, sortOrder: number) => ({
  workstreamId,
  name: workstreamId,
  lifecycle: "active" as const,
  progress: { state: "progressing" as const },
  delivery: "none-observed" as const,
  freshness: "current" as const,
  sortOrder,
  version: 1,
  updatedAt: "2026-09-12T12:00:00Z",
});
const ref = (nativeReferenceId: string, environmentId: string, threadId: string) => ({
  native_reference_id: nativeReferenceId,
  owner_id: "owner",
  identity: {
    provider: "t3",
    source_instance_id: environmentId,
    resource_kind: "thread",
    id_kind: "internal" as const,
    native_id: threadId,
    account_provenance: { kind: "not_account_scoped" as const },
  },
  pr_locator: null,
  registration: {
    state: "attested" as const,
    attestation_version: 1,
    attested_at: "2026-09-12T11:00:00Z",
    expires_at: "2026-09-13T12:00:00Z",
    evidence: {
      provider: "t3",
      source_instance_id: environmentId,
      authority_namespace: environmentId,
      native_id: threadId,
      store_generation: 1,
      evidence_sha256: "a".repeat(64),
    },
  },
  created_at: "2026-09-12T12:00:00Z",
  created_by: { principal_id: "principal" },
  created_registry_version: 1,
});
const member = (
  id: string,
  workstreamId: string,
  referenceId: string,
  kind: "primary" | "secondary" = "primary",
) => ({
  membership_id: id,
  workstream_id: workstreamId,
  native_reference_id: referenceId,
  kind,
  opened: {
    at: "2026-09-12T12:00:00Z",
    actor: { principal_id: "principal" },
    command_id: "command-open-0001",
    registry_version: 1,
  },
  closed: null,
});

describe("native Workstream thread grouping", () => {
  it("resolves the Sidebar secondary label through the same colon-safe key and deduplicates links", () => {
    const thread = { environmentId: "a:b", id: "c" };
    const other = { environmentId: "a", id: "b:c" };
    const input = {
      workstreams: [{ ...ws("secondary", 0), name: "Secondary label" }],
      references: [ref("reference", thread.environmentId, thread.id)],
      memberships: [
        member("first", "secondary", "reference", "secondary"),
        member("duplicate", "secondary", "reference", "secondary"),
      ],
      threads: [thread, other],
      trustedNow,
      trustedEnvironments: trustedEnvironments("a:b", "a"),
    };
    const grouping = groupNativeThreadsByWorkstream(input);
    expect(secondaryNativeWorkstreamLabels(grouping, thread)).toEqual(["Secondary label"]);
    expect(secondaryNativeWorkstreamLabels(grouping, other)).toEqual([]);
    expect(
      secondaryNativeWorkstreamLabels(
        groupNativeThreadsByWorkstream({ ...input, trustedEnvironments: new Map() }),
        thread,
      ),
    ).toEqual([]);
    expect(grouping.ordered).toEqual([thread, other]);
  });
  it("groups active projection placements once and preserves native actions and missing-group fallback", () => {
    const nativeAction = () => "native-action";
    const threads = [
      { environmentId: "env:a", id: "thread:a", projectId: "native-project", nativeAction },
      { environmentId: "env", id: "a:thread:a", projectId: "other-native-project", nativeAction },
    ];
    const placement = {
      membership_id: "m:1",
      workstream_id: "ws:a",
      native_reference_id: "ref:a",
      kind: "primary" as const,
      source_instance_id: "env:a",
      native_thread_id: "thread:a",
      attestation_version: 1,
      attested_at: "2026-09-12T11:00:00Z",
      expires_at: "2026-09-13T12:00:00Z",
      evidence_sha256: "a".repeat(64),
      source_binding_version: 1,
      authority_namespace: "env:a",
      store_generation: 1,
    };
    const input = {
      workstreams: [ws("ws:a", 0)],
      threads,
      placements: [
        placement,
        {
          ...placement,
          membership_id: "m:2",
          workstream_id: "ws:secondary",
          kind: "secondary" as const,
        },
      ],
      trustedNow,
      trustedEnvironments: trustedEnvironments("env:a"),
    };
    const result = groupNativeThreadsByWorkstream(input);
    expect(result.groups[0]?.threads).toEqual([threads[0]]);
    expect(result.ungrouped).toEqual([threads[1]]);
    expect(result.ordered).toHaveLength(2);
    expect(result.groups[0]?.threads[0]?.nativeAction).toBe(nativeAction);
    expect(
      result.secondaryWorkstreamIdsByKey.get(nativeWorkstreamThreadKey("env:a", "thread:a")),
    ).toEqual(["ws:secondary"]);
    expect(groupNativeThreadsByWorkstream({ ...input, workstreams: [] }).ungrouped).toEqual(
      threads,
    );
    expect(
      groupNativeThreadsByWorkstream({ ...input, trustedEnvironments: new Map() }).ungrouped,
    ).toEqual(threads);
    for (const changed of [
      { ...placement, authority_namespace: "other" },
      { ...placement, store_generation: 2 },
      { ...placement, expires_at: trustedNow },
      { ...placement, attested_at: "2026-09-13T12:00:00Z" },
    ]) {
      expect(groupNativeThreadsByWorkstream({ ...input, placements: [changed] }).ungrouped).toEqual(
        threads,
      );
    }
  });
  it("places each primary once in owner order and preserves native thread order", () => {
    const threads = [
      { environmentId: "env-b", id: "same", projectId: "project-b" },
      { environmentId: "env-a", id: "same", projectId: "project-a" },
      { environmentId: "env-a", id: "free", projectId: "project-a" },
    ];
    const result = groupNativeThreadsByWorkstream({
      workstreams: [ws("ws-b", 0), ws("ws-a", 0)],
      references: [ref("ref-a", "env-a", "same"), ref("ref-b", "env-b", "same")],
      memberships: [member("m-a", "ws-a", "ref-a"), member("m-b", "ws-b", "ref-b")],
      threads,
      trustedNow,
      trustedEnvironments: trustedEnvironments("env-a", "env-b"),
    });
    expect(result.groups.map((group) => group.workstream.workstreamId)).toEqual(["ws-b", "ws-a"]);
    expect(result.ordered.map((thread) => `${thread.environmentId}:${thread.id}`)).toEqual([
      "env-b:same",
      "env-a:same",
      "env-a:free",
    ]);
    expect(new Set(result.ordered).size).toBe(3);
    expect(result.groups[1]?.threads[0]?.projectId).toBe("project-a");
  });

  it("ignores closed and foreign references, records secondary links, and exposes conflicts", () => {
    const closed = {
      ...member("closed", "ws-a", "ref-closed"),
      closed: {
        ...member("x", "x", "x").opened,
        reason: "removed" as const,
        other_reason: null,
      },
    };
    const foreign = {
      ...ref("foreign", "env", "foreign"),
      identity: { ...ref("foreign", "env", "foreign").identity, provider: "github" },
    };
    const result = groupNativeThreadsByWorkstream({
      workstreams: [ws("ws-a", 0), ws("ws-b", 1)],
      references: [ref("shared", "env", "thread"), ref("ref-closed", "env", "closed"), foreign],
      memberships: [
        member("p-a", "ws-a", "shared"),
        member("p-b", "ws-b", "shared"),
        member("s", "ws-b", "shared", "secondary"),
        closed,
        member("f", "ws-a", "foreign"),
      ],
      threads: [
        { environmentId: "env", id: "thread" },
        { environmentId: "env", id: "closed" },
        { environmentId: "env", id: "foreign" },
      ],
      trustedNow,
      trustedEnvironments: trustedEnvironments("env"),
    });
    expect(result.groups).toHaveLength(0);
    expect(result.ungrouped).toHaveLength(3);
    const key = nativeWorkstreamThreadKey("env", "thread");
    expect(result.conflictingKeys.has(key)).toBe(true);
    expect(result.secondaryWorkstreamIdsByKey.get(key)).toEqual(["ws-b"]);
  });

  it("fails closed for untrusted registration states, expiry, and attestation mismatches", () => {
    const valid = ref("valid", "env", "thread");
    const variants = [
      { ...valid, registration: { ...valid.registration, state: "verification-pending" as const } },
      { ...valid, registration: { ...valid.registration, state: "quarantined" as const } },
      { ...valid, registration: { ...valid.registration, expires_at: trustedNow } },
      {
        ...valid,
        registration: {
          ...valid.registration,
          evidence: { ...valid.registration.evidence, native_id: "different" },
        },
      },
      {
        ...valid,
        registration: {
          ...valid.registration,
          evidence: { ...valid.registration.evidence, store_generation: 2 },
        },
      },
      {
        ...valid,
        registration: {
          ...valid.registration,
          evidence: { ...valid.registration.evidence, authority_namespace: "other" },
        },
      },
      { ...valid, identity: { ...valid.identity, id_kind: "external" as const } },
      {
        ...valid,
        identity: {
          ...valid.identity,
          account_provenance: {
            kind: "account" as const,
            account_id: "account",
            account_namespace: "namespace",
          },
        },
      },
    ];
    for (const [index, reference] of variants.entries()) {
      const result = groupNativeThreadsByWorkstream({
        workstreams: [ws("ws", 0)],
        references: [{ ...reference, native_reference_id: `ref-${index}` }],
        memberships: [member(`m-${index}`, "ws", `ref-${index}`)],
        threads: [{ environmentId: "env", id: "thread" }],
        trustedNow,
        trustedEnvironments: trustedEnvironments("env"),
      });
      expect(result.groups, String(index)).toHaveLength(0);
    }
    expect(
      groupNativeThreadsByWorkstream({
        workstreams: [ws("ws", 0)],
        references: [valid],
        memberships: [member("missing-trust", "ws", "valid")],
        threads: [{ environmentId: "env", id: "thread" }],
        trustedNow,
        trustedEnvironments: new Map(),
      }).groups,
    ).toHaveLength(0);
  });

  it("keeps colon-bearing environment and thread tuples collision-free", () => {
    const result = groupNativeThreadsByWorkstream({
      workstreams: [ws("ws-left", 0), ws("ws-right", 1)],
      references: [ref("left", "a:b", "c"), ref("right", "a", "b:c")],
      memberships: [member("m-left", "ws-left", "left"), member("m-right", "ws-right", "right")],
      threads: [
        { environmentId: "a:b", id: "c" },
        { environmentId: "a", id: "b:c" },
      ],
      trustedNow,
      trustedEnvironments: trustedEnvironments("a:b", "a"),
    });
    expect(nativeWorkstreamThreadKey("a:b", "c")).not.toBe(nativeWorkstreamThreadKey("a", "b:c"));
    expect(result.groups.map((group) => group.workstream.workstreamId)).toEqual([
      "ws-left",
      "ws-right",
    ]);
  });
});

describe("native Workstream thread grouping from an assignment index", () => {
  const index = (
    references: readonly NativeReference[],
    memberships: readonly MembershipEpisode[],
    complete = true,
  ) =>
    buildWorkstreamAssignmentIndex({
      ownerId: "owner",
      registryVersion: 3,
      references,
      memberships,
      complete,
    });
  const expired = (reference: NativeReference): NativeReference => ({
    ...reference,
    registration: {
      ...reference.registration,
      state: "expired",
      expires_at: "2026-09-12T11:05:00Z",
    },
  });
  const assurance = (
    grouping: ReturnType<typeof groupNativeThreadsByWorkstream>,
    environmentId: string,
    threadId: string,
  ) => grouping.assuranceByKey?.get(nativeWorkstreamThreadKey(environmentId, threadId));

  it("groups expired and remote stored assignments as recorded and current proof as verified", () => {
    const threads = [
      { environmentId: "env", id: "current" },
      { environmentId: "env", id: "expired" },
      { environmentId: "remote", id: "remote" },
    ];
    const result = groupNativeThreadsByWorkstream({
      workstreams: [ws("ws-a", 0), ws("ws-b", 1)],
      threads,
      trustedNow,
      trustedEnvironments: trustedEnvironments("env"),
      assignmentIndex: index(
        [
          ref("current", "env", "current"),
          expired(ref("expired", "env", "expired")),
          expired(ref("remote", "remote", "remote")),
        ],
        [
          member("m-current", "ws-a", "current"),
          member("m-expired", "ws-a", "expired"),
          member("m-remote", "ws-b", "remote"),
        ],
      ),
    });
    expect(result.groups.map((group) => group.threads)).toEqual([
      [threads[0], threads[1]],
      [threads[2]],
    ]);
    expect(result.ungrouped).toEqual([]);
    expect(assurance(result, "env", "current")).toBe("verified");
    expect(assurance(result, "env", "expired")).toBe("recorded");
    expect(assurance(result, "remote", "remote")).toBe("recorded");
  });

  it("verifies only from a current trusted placement for the indexed primary membership", () => {
    const thread = { environmentId: "env", id: "thread" };
    const placement = {
      membership_id: "m",
      workstream_id: "ws",
      native_reference_id: "ref",
      kind: "primary" as const,
      source_instance_id: "env",
      native_thread_id: "thread",
      attestation_version: 1,
      attested_at: "2026-09-12T11:59:00Z",
      expires_at: "2026-09-12T12:04:00Z",
      evidence_sha256: "a".repeat(64),
      source_binding_version: 1,
      authority_namespace: "env",
      store_generation: 1,
    };
    const input = {
      workstreams: [ws("ws", 0)],
      threads: [thread],
      trustedNow,
      trustedEnvironments: trustedEnvironments("env"),
      assignmentIndex: index([expired(ref("ref", "env", "thread"))], [member("m", "ws", "ref")]),
    };
    expect(
      assurance(
        groupNativeThreadsByWorkstream({ ...input, placements: [placement] }),
        "env",
        "thread",
      ),
    ).toBe("verified");
    for (const changed of [
      { ...placement, membership_id: "other" },
      { ...placement, workstream_id: "ws-other" },
      { ...placement, expires_at: trustedNow },
      { ...placement, store_generation: 2 },
    ]) {
      const result = groupNativeThreadsByWorkstream({ ...input, placements: [changed] });
      expect(assurance(result, "env", "thread")).toBe("recorded");
      expect(result.groups[0]?.threads).toEqual([thread]);
    }
  });

  it("keeps proof attested after the verification clock grouped as recorded", () => {
    const thread = { environmentId: "env", id: "thread" };
    const reference = ref("ref", "env", "thread");
    const ahead = {
      ...reference,
      registration: {
        ...reference.registration,
        attested_at: "2026-09-12T12:00:30Z",
        expires_at: "2026-09-12T12:05:30Z",
      },
    };
    const input = {
      workstreams: [ws("ws", 0)],
      threads: [thread],
      trustedNow,
      trustedEnvironments: trustedEnvironments("env"),
      assignmentIndex: index([ahead], [member("m", "ws", "ref")]),
    };
    const coarse = groupNativeThreadsByWorkstream(input);
    expect(coarse.groups[0]?.threads).toEqual([thread]);
    expect(assurance(coarse, "env", "thread")).toBe("recorded");
    const current = groupNativeThreadsByWorkstream({
      ...input,
      verificationNow: "2026-09-12T12:01:00Z",
    });
    expect(current.groups[0]?.threads).toEqual([thread]);
    expect(assurance(current, "env", "thread")).toBe("verified");
  });

  it("flags two open primaries and known namespace or generation mismatches as conflicts", () => {
    const threads = [
      { environmentId: "env", id: "shared" },
      { environmentId: "env", id: "namespace" },
      { environmentId: "env", id: "generation" },
    ];
    const namespace = ref("namespace", "env", "namespace");
    const generation = ref("generation", "env", "generation");
    const result = groupNativeThreadsByWorkstream({
      workstreams: [ws("ws-a", 0), ws("ws-b", 1)],
      threads,
      trustedNow,
      trustedEnvironments: trustedEnvironments("env"),
      assignmentIndex: index(
        [
          ref("shared-a", "env", "shared"),
          ref("shared-b", "env", "shared"),
          {
            ...namespace,
            registration: {
              ...namespace.registration,
              evidence: { ...namespace.registration.evidence, authority_namespace: "old" },
            },
          },
          expired({
            ...generation,
            registration: {
              ...generation.registration,
              evidence: { ...generation.registration.evidence, store_generation: 2 },
            },
          }),
        ],
        [
          member("p-a", "ws-a", "shared-a"),
          member("p-b", "ws-b", "shared-b"),
          member("p-namespace", "ws-a", "namespace"),
          member("p-generation", "ws-a", "generation"),
        ],
      ),
    });
    expect(result.groups).toEqual([]);
    expect(result.ungrouped).toEqual(threads);
    for (const thread of threads) {
      expect(assurance(result, thread.environmentId, thread.id)).toBe("conflict");
      expect(
        result.conflictingKeys.has(nativeWorkstreamThreadKey(thread.environmentId, thread.id)),
      ).toBe(true);
    }
  });

  it("ignores closed episodes and assignments without a visible native thread", () => {
    const closed = {
      ...member("closed", "ws", "closed"),
      closed: { ...member("x", "x", "x").opened, reason: "removed" as const, other_reason: null },
    };
    const result = groupNativeThreadsByWorkstream({
      workstreams: [ws("ws", 0)],
      threads: [{ environmentId: "env", id: "closed" }],
      trustedNow,
      trustedEnvironments: trustedEnvironments("env"),
      assignmentIndex: index(
        [ref("closed", "env", "closed"), ref("missing", "env", "missing")],
        [closed, member("missing", "ws", "missing"), member("s", "ws", "missing", "secondary")],
      ),
    });
    expect(result.groups).toEqual([]);
    expect([...(result.assuranceByKey?.entries() ?? [])]).toEqual([
      [nativeWorkstreamThreadKey("env", "closed"), "unassigned"],
    ]);
    expect(result.secondaryWorkstreamIdsByKey.size).toBe(0);
    expect(result.groupedKeys.size).toBe(0);
  });

  it("confirms unassigned only from a complete index and labels secondary links", () => {
    const threads = [
      { environmentId: "env", id: "free" },
      { environmentId: "env", id: "linked" },
    ];
    const input = {
      workstreams: [{ ...ws("ws", 0), name: "Linked" }],
      threads,
      trustedNow,
      trustedEnvironments: trustedEnvironments("env"),
    };
    const references = [ref("linked", "env", "linked")];
    const memberships = [member("s", "ws", "linked", "secondary")];
    const complete = groupNativeThreadsByWorkstream({
      ...input,
      assignmentIndex: index(references, memberships),
    });
    expect(complete.ungrouped).toEqual(threads);
    expect(assurance(complete, "env", "free")).toBe("unassigned");
    expect(assurance(complete, "env", "linked")).toBe("unassigned");
    expect(secondaryNativeWorkstreamLabels(complete, threads[1]!)).toEqual(["Linked"]);
    const partial = groupNativeThreadsByWorkstream({
      ...input,
      assignmentIndex: index(references, memberships, false),
    });
    expect(partial.ungrouped).toEqual(threads);
    expect(assurance(partial, "env", "free")).toBe("unknown");
    expect(assurance(partial, "env", "linked")).toBe("unknown");
  });

  it("leaves a primary in a workstream missing from the list as unknown", () => {
    const thread = { environmentId: "env", id: "thread" };
    const result = groupNativeThreadsByWorkstream({
      workstreams: [],
      threads: [thread],
      trustedNow,
      trustedEnvironments: trustedEnvironments("env"),
      assignmentIndex: index([ref("ref", "env", "thread")], [member("m", "ws", "ref")]),
    });
    expect(result.ungrouped).toEqual([thread]);
    expect(assurance(result, "env", "thread")).toBe("unknown");
  });

  it("adds no assurance output when no index is supplied", () => {
    const result = groupNativeThreadsByWorkstream({
      workstreams: [ws("ws", 0)],
      references: [ref("ref", "env", "thread")],
      memberships: [member("m", "ws", "ref")],
      threads: [{ environmentId: "env", id: "thread" }],
      trustedNow,
      trustedEnvironments: trustedEnvironments("env"),
    });
    expect(result.groups).toHaveLength(1);
    expect("assuranceByKey" in result).toBe(false);
  });
});
