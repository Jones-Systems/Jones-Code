// @effect-diagnostics-next-line nodeBuiltinImport:off -- Independently verify the digest of the exact serialized fixture bytes.
import * as NodeCrypto from "node:crypto";
import { describe, expect, it } from "@effect/vitest";
import { RunId, ThreadId } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { OrchestrationEffectRequestV2 } from "../../orchestration-v2/EffectOutbox.ts";
import { serverUpdateContinuationEffect } from "../../orchestration-v2/ServerUpdateContinuationPolicy.ts";
import {
  decodeOfflinePreparationPlan,
  encodeOfflinePreparationPlan,
  OfflinePreparationPlanError,
  offlinePreparationPlanLimits,
  type OfflinePreparationPlan,
} from "./offlinePreparationPlan.ts";

const timestamp = "2026-10-11T12:00:00.000Z";
const preparationId = "22222222-2222-4222-8222-222222222222";
const encodeRequest = Schema.encodeSync(Schema.fromJsonString(OrchestrationEffectRequestV2));
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const bytes = (value: string) => new TextEncoder().encode(value);
type Row = OfflinePreparationPlan["expectedRows"][number];
function row(id: string, thread: string, run: string): Row {
  const effect = serverUpdateContinuationEffect({
    preparationId: id,
    threadId: ThreadId.make(thread),
    sourceRunId: RunId.make(run),
    optedInOnly: true,
  });
  return {
    effect_id: effect.id,
    command_id: effect.commandId,
    thread_id: effect.threadId,
    effect_type: effect.request.type,
    payload_json: encodeRequest(effect.request),
    status: "pending",
    attempt_count: 0,
    available_at: timestamp,
    created_at: timestamp,
    updated_at: timestamp,
    lease_owner: null,
    lease_expires_at: null,
    completed_at: null,
    last_error: null,
  };
}
function fixture(): OfflinePreparationPlan {
  const previous = row("existing:thread:previous", "thread:previous", "run:previous");
  return {
    id: "11111111-1111-4111-8111-111111111111",
    timestamp,
    source: "bd20d2d2fc0ba89797698192b877a7f1312f8fe3",
    snapshotSha256: "a".repeat(64),
    settingsSha256: "b".repeat(64),
    selections: [
      {
        threadId: ThreadId.make("thread:new"),
        runId: RunId.make("run:new"),
        existingId: null,
        projectionSha256: "c".repeat(64),
      },
      {
        threadId: ThreadId.make("thread:previous"),
        runId: RunId.make("run:previous"),
        existingId: previous.effect_id,
        projectionSha256: "d".repeat(64),
      },
    ],
    expectedRows: [row(preparationId, "thread:new", "run:new")],
    preservedRows: [
      previous,
      {
        ...row("unrelated", "thread:unrelated", "run:unrelated"),
        effect_type: "provider-turn.start",
        payload_json: "unrelated malformed historical payload",
        status: "failed",
        attempt_count: 2,
        last_error: "retained evidence",
      },
    ],
  };
}
function refused(run: () => unknown, reason?: OfflinePreparationPlanError["reason"]) {
  expect(run).toThrow(OfflinePreparationPlanError);
  if (reason !== undefined) expect(run).toThrow("Invalid offline preparation plan: " + reason);
}
const alterExpected = (
  plan: OfflinePreparationPlan,
  fields: Readonly<Record<string, unknown>>,
) => ({
  ...plan,
  expectedRows: [{ ...plan.expectedRows[0]!, ...fields }],
});

describe("durable offline preparation plan codec", () => {
  it("reloads fresh immutable objects with unchanged policy-produced IDs, timestamp and exact digest", () => {
    const original = fixture();
    const encoded = encodeOfflinePreparationPlan(original);
    expect(new TextDecoder().decode(encoded)).toBe(encodeJson(original));
    const first = decodeOfflinePreparationPlan(encoded);
    const reloaded = decodeOfflinePreparationPlan(Uint8Array.from(encoded));
    expect(reloaded.plan).toEqual(original);
    expect(reloaded.plan).not.toBe(first.plan);
    expect(reloaded.plan.expectedRows[0]).not.toBe(first.plan.expectedRows[0]);
    expect(reloaded.sha256).toBe(NodeCrypto.createHash("sha256").update(encoded).digest("hex"));
    expect(encodeOfflinePreparationPlan(reloaded.plan)).toEqual(encoded);
    for (const value of [
      reloaded,
      reloaded.plan,
      reloaded.plan.selections,
      reloaded.plan.expectedRows,
      reloaded.plan.preservedRows,
      ...reloaded.plan.selections,
      ...reloaded.plan.expectedRows,
      ...reloaded.plan.preservedRows,
    ])
      expect(Object.isFrozen(value)).toBe(true);
    encoded.fill(0);
    expect(reloaded.plan).toEqual(original);
  });

  it("hashes valid changed data differently without pretending the digest grants admission", () => {
    const original = fixture();
    const changed = { ...original, id: "33333333-3333-4333-8333-333333333333" };
    expect(decodeOfflinePreparationPlan(encodeOfflinePreparationPlan(changed)).sha256).not.toBe(
      decodeOfflinePreparationPlan(encodeOfflinePreparationPlan(original)).sha256,
    );
  });

  it("preserves opaque rows and legacy existing markers without newer optional flags", () => {
    const plan = fixture();
    const historical = {
      ...plan,
      preservedRows: [
        {
          ...plan.preservedRows[0]!,
          payload_json:
            '{ "sourceRunId": "run:previous", "preparedForRestart": 1, "historical": true }',
        },
        plan.preservedRows[1]!,
      ],
    };
    const reloaded = decodeOfflinePreparationPlan(encodeOfflinePreparationPlan(historical)).plan;
    expect(reloaded.preservedRows).toEqual(historical.preservedRows);
    expect(reloaded.selections[1]?.existingId).toBe(historical.preservedRows[0]?.effect_id);
  });

  it("accepts a bounded empty plan without fabricating continuation rows", () => {
    const plan = { ...fixture(), selections: [], expectedRows: [], preservedRows: [] };
    expect(decodeOfflinePreparationPlan(encodeOfflinePreparationPlan(plan)).plan).toEqual(plan);
  });

  it.each([
    ["leading whitespace", (raw: string) => " " + raw],
    ["trailing newline", (raw: string) => raw + "\n"],
    ["duplicate key", (raw: string) => raw.replace('{"id":', '{"id":"ignored","id":')],
    ["BOM", (raw: string) => "\ufeff" + raw],
  ] as const)("rejects %s instead of silently changing its digest", (_name, change) => {
    const raw = new TextDecoder().decode(encodeOfflinePreparationPlan(fixture()));
    refused(() => decodeOfflinePreparationPlan(bytes(change(raw))));
  });

  it("rejects invalid UTF-8 and reordered JSON keys", () => {
    refused(() => decodeOfflinePreparationPlan(new Uint8Array([0xc3, 0x28])), "encoding");
    const { timestamp: planTimestamp, ...rest } = fixture();
    refused(
      () => decodeOfflinePreparationPlan(bytes(encodeJson({ timestamp: planTimestamp, ...rest }))),
      "noncanonical",
    );
  });

  it.each([
    { name: "unknown top-level field", change: (plan) => ({ ...plan, admitted: true }) },
    {
      name: "unknown selection field",
      change: (plan) => ({
        ...plan,
        selections: [{ ...plan.selections[0]!, accepted: true }, plan.selections[1]!],
      }),
    },
    { name: "unknown row field", change: (plan) => alterExpected(plan, { admitted: true }) },
    { name: "invalid plan id", change: (plan) => ({ ...plan, id: "not-a-uuid" }) },
    { name: "foreign source", change: (plan) => ({ ...plan, source: "e".repeat(40) }) },
    {
      name: "impossible date",
      change: (plan) => ({ ...plan, timestamp: "2026-02-30T12:00:00.000Z" }),
    },
    {
      name: "noncanonical timestamp",
      change: (plan) => ({ ...plan, timestamp: "2026-10-11T12:00:00Z" }),
    },
    {
      name: "invalid baseline digest",
      change: (plan) => ({ ...plan, snapshotSha256: "x".repeat(64) }),
    },
    {
      name: "invalid settings digest",
      change: (plan) => ({ ...plan, settingsSha256: "a".repeat(63) }),
    },
    {
      name: "invalid projection digest",
      change: (plan) => ({
        ...plan,
        selections: [{ ...plan.selections[0]!, projectionSha256: "" }, plan.selections[1]!],
      }),
    },
    {
      name: "normalized identifier",
      change: (plan) => ({
        ...plan,
        selections: [{ ...plan.selections[0]!, threadId: " thread:new " }, plan.selections[1]!],
      }),
    },
    {
      name: "oversized identifier",
      change: (plan) =>
        alterExpected(plan, { thread_id: "t".repeat(offlinePreparationPlanLimits.idBytes + 1) }),
    },
    {
      name: "UTF-8 oversized identifier",
      change: (plan) =>
        alterExpected(plan, { thread_id: "é".repeat(offlinePreparationPlanLimits.idBytes) }),
    },
    {
      name: "duplicate thread",
      change: (plan) => ({ ...plan, selections: [plan.selections[0]!, plan.selections[0]!] }),
    },
    {
      name: "duplicate run",
      change: (plan) => ({
        ...plan,
        selections: [
          plan.selections[0]!,
          { ...plan.selections[1]!, runId: plan.selections[0]!.runId },
        ],
      }),
    },
    {
      name: "duplicate expected id",
      change: (plan) => ({ ...plan, expectedRows: [plan.expectedRows[0]!, plan.expectedRows[0]!] }),
    },
    {
      name: "duplicate preserved id",
      change: (plan) => ({
        ...plan,
        preservedRows: [plan.preservedRows[0]!, plan.preservedRows[0]!],
      }),
    },
    {
      name: "preserved id collision",
      change: (plan) => ({
        ...plan,
        preservedRows: [...plan.preservedRows, plan.expectedRows[0]!],
      }),
    },
    {
      name: "preserved command collision",
      change: (plan) => ({
        ...plan,
        preservedRows: [
          ...plan.preservedRows,
          { ...plan.expectedRows[0]!, effect_id: "other-effect" },
        ],
      }),
    },
    {
      name: "existing continuation collision",
      change: (plan) => ({
        ...plan,
        preservedRows: [...plan.preservedRows, row("existing:collision", "thread:new", "run:new")],
      }),
    },
    {
      name: "unselected expected row",
      change: (plan) => ({ ...plan, selections: [plan.selections[1]!] }),
    },
    { name: "missing expected row", change: (plan) => ({ ...plan, expectedRows: [] }) },
    {
      name: "missing existing marker",
      change: (plan) => ({ ...plan, preservedRows: [plan.preservedRows[1]!] }),
    },
    {
      name: "wrong existing thread",
      change: (plan) => ({
        ...plan,
        preservedRows: [
          { ...plan.preservedRows[0]!, thread_id: "other-thread" },
          plan.preservedRows[1]!,
        ],
      }),
    },
    {
      name: "advanced existing marker",
      change: (plan) => ({
        ...plan,
        preservedRows: [{ ...plan.preservedRows[0]!, status: "running" }, plan.preservedRows[1]!],
      }),
    },
    {
      name: "wrong existing run",
      change: (plan) => ({
        ...plan,
        preservedRows: [
          {
            ...plan.preservedRows[0]!,
            payload_json: '{"sourceRunId":"other","preparedForRestart":true}',
          },
          plan.preservedRows[1]!,
        ],
      }),
    },
    {
      name: "ambiguous existing marker keys",
      change: (plan) => ({
        ...plan,
        preservedRows: [
          {
            ...plan.preservedRows[0]!,
            payload_json:
              '{"sourceRunId":"other","sourceRunId":"run:previous","preparedForRestart":true}',
          },
          plan.preservedRows[1]!,
        ],
      }),
    },
    {
      name: "unprepared existing marker",
      change: (plan) => ({
        ...plan,
        preservedRows: [
          {
            ...plan.preservedRows[0]!,
            payload_json: '{"sourceRunId":"run:previous","preparedForRestart":false}',
          },
          plan.preservedRows[1]!,
        ],
      }),
    },
  ] satisfies ReadonlyArray<{ name: string; change: (plan: OfflinePreparationPlan) => unknown }>)(
    "rejects $name",
    ({ change }) => refused(() => encodeOfflinePreparationPlan(change(fixture()))),
  );

  it.each([
    ["effect id", { effect_id: "effect:server-update-continuation:invalid" }],
    ["command relationship", { command_id: "command:server-update-prepare:different" }],
    ["thread relationship", { thread_id: "thread:other" }],
    ["effect type", { effect_type: "provider-turn.start" }],
    ["status", { status: "running" }],
    ["attempt", { attempt_count: 1 }],
    ["available time", { available_at: "2026-10-11T12:00:01.000Z" }],
    ["creation time", { created_at: "2026-10-11T12:00:01.000Z" }],
    ["update time", { updated_at: "2026-10-11T12:00:01.000Z" }],
    ["lease", { lease_owner: "worker" }],
    ["lease expiry", { lease_expires_at: timestamp }],
    ["completion", { completed_at: timestamp }],
    ["error", { last_error: "unexpected" }],
    [
      "request run",
      {
        payload_json:
          '{"type":"provider-runtime.continue","sourceRunId":"run:other","preparedForRestart":true,"continueWithoutPreference":false}',
      },
    ],
    [
      "request preference",
      {
        payload_json:
          '{"type":"provider-runtime.continue","sourceRunId":"run:new","preparedForRestart":true,"continueWithoutPreference":true}',
      },
    ],
    [
      "request extra",
      {
        payload_json:
          '{"type":"provider-runtime.continue","sourceRunId":"run:new","preparedForRestart":true,"continueWithoutPreference":false,"extra":true}',
      },
    ],
    [
      "noncanonical request",
      {
        payload_json:
          '{ "type":"provider-runtime.continue","sourceRunId":"run:new","preparedForRestart":true,"continueWithoutPreference":false}',
      },
    ],
  ] as const)("rejects changed expected-row %s", (_name, fields) => {
    refused(() => encodeOfflinePreparationPlan(alterExpected(fixture(), fields)));
  });

  it("bounds raw input, aggregate output, row counts and individual payloads", () => {
    refused(() => decodeOfflinePreparationPlan(new Uint8Array()), "size");
    refused(
      () => decodeOfflinePreparationPlan(new Uint8Array(offlinePreparationPlanLimits.bytes + 1)),
      "size",
    );
    const plan = fixture();
    refused(
      () =>
        encodeOfflinePreparationPlan({
          ...plan,
          selections: Array.from(
            { length: offlinePreparationPlanLimits.selections + 1 },
            () => plan.selections[0],
          ),
        }),
      "size",
    );
    refused(
      () =>
        encodeOfflinePreparationPlan({
          ...plan,
          preservedRows: Array.from(
            { length: offlinePreparationPlanLimits.preservedRows + 1 },
            () => plan.preservedRows[0],
          ),
        }),
      "size",
    );
    const maximumPayload = "x".repeat(offlinePreparationPlanLimits.payloadBytes);
    const bounded = {
      ...plan,
      preservedRows: [
        plan.preservedRows[0]!,
        { ...plan.preservedRows[1]!, payload_json: maximumPayload },
      ],
    };
    expect(
      decodeOfflinePreparationPlan(encodeOfflinePreparationPlan(bounded)).plan.preservedRows[1]
        ?.payload_json,
    ).toBe(maximumPayload);
    refused(
      () =>
        encodeOfflinePreparationPlan({
          ...plan,
          preservedRows: [{ ...plan.preservedRows[0]!, payload_json: maximumPayload + "x" }],
        }),
      "shape",
    );
    refused(
      () =>
        encodeOfflinePreparationPlan({
          ...plan,
          preservedRows: Array.from({ length: 33 }, (_, index) => ({
            ...plan.preservedRows[1]!,
            effect_id: `effect:${index}`,
            payload_json: maximumPayload,
          })),
        }),
      "size",
    );
  });
});
