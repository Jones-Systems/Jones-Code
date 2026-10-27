// @effect-diagnostics globalDate:off - Client reconciliation compares recorded receipt clocks; only target/native evidence supplies outcomes.
import * as Schema from "effect/Schema";
import { EnvironmentId } from "@t3tools/contracts";
import {
  JonesOperationId,
  JonesOperationStartInput,
  JonesOperationReceipt,
  JonesSourceSha,
} from "@t3tools/contracts/jones/jonesOperations";
import { validateReceipt } from "../installPlan/model.ts";

export interface JonesActionStorage {
  readonly getItem: (key: string) => string | null | Promise<string | null>;
  readonly setItem: (key: string, value: string) => void | Promise<void>;
  readonly withLock?: <A>(work: () => Promise<A>) => Promise<A>;
}

const Text = Schema.String.check(Schema.isMaxLength(1024));
export const JonesActionDuty = Schema.Struct({
  id: JonesOperationId,
  environmentId: EnvironmentId,
  kind: Schema.Literals(["legacy", "operation", "stop", "native"]),
  action: Schema.Literals(["download", "download-exact", "replace-exact", "install", "stop"]),
  targetSource: JonesSourceSha,
  stagedHandle: Schema.optionalKey(Text),
  currentVersion: Schema.optionalKey(Text),
  operationId: Schema.optionalKey(JonesOperationId),
  stopId: Schema.optionalKey(JonesOperationId),
  request: Schema.optionalKey(JonesOperationStartInput),
  receipt: Schema.optionalKey(JonesOperationReceipt),
  native: Schema.optionalKey(
    Schema.Struct({
      commandId: JonesOperationId,
      bodyHash: Schema.optionalKey(Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/))),
      requestType: Schema.Literals(["start", "edit", "laptop", "claim"]),
      planId: Schema.optionalKey(JonesOperationId),
      expectedRevision: Schema.optionalKey(Schema.Number),
    }),
  ),
});
export type JonesActionDuty = typeof JonesActionDuty.Type;
const Document = Schema.Struct({
  schema: Schema.Literal(1),
  duties: Schema.Array(JonesActionDuty).check(Schema.isMaxLength(64)),
  history: Schema.optionalKey(Schema.Array(JonesActionDuty).check(Schema.isMaxLength(64))),
});
export const JONES_ACTION_JOURNAL_KEY = "jones.update-duties.v1";
const JONES_ACTION_JOURNAL_BYTES = 128 * 1024;
const terminal = new Set([
  "completed",
  "rolled-back",
  "stopped",
  "refused",
  "failed",
  "blocked",
  "fenced",
  "interrupted",
]);
export function canonicalJonesCommand(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value) ?? "null";
  if (Array.isArray(value)) return `[${value.map(canonicalJonesCommand).join(",")}]`;
  return `{${Object.keys(value)
    .sort()
    .filter((key) => (value as Record<string, unknown>)[key] !== undefined)
    .map(
      (key) =>
        `${JSON.stringify(key)}:${canonicalJonesCommand((value as Record<string, unknown>)[key])}`,
    )
    .join(",")}}`;
}
const settledJonesReceipt = (receipt: JonesOperationReceipt) =>
  terminal.has(receipt.phase) && receipt.code !== "launcher-unconfirmed";

export function validateJonesStopReceipt(
  previous: JonesOperationReceipt,
  next: JonesOperationReceipt,
) {
  previous = correlationReceipt(previous);
  next = correlationReceipt(next);
  if (validateReceipt(previous, next)) return true;
  if (
    !terminal.has(previous.phase) ||
    next.phase !== previous.phase ||
    previous.stop === undefined ||
    next.stop?.stopId !== previous.stop.stopId ||
    next.stop.requestedAt !== previous.stop.requestedAt ||
    (previous.stop.result !== undefined && previous.stop.result !== "held") ||
    !["stopped", "too-late", "not-stoppable-phase"].includes(next.stop.result ?? "") ||
    Date.parse(next.updatedAt) < Date.parse(previous.updatedAt)
  )
    return false;
  const { updatedAt: _at, stop: _stop, code: _code, ...stable } = previous;
  return validateReceipt(
    {
      ...stable,
      updatedAt: next.updatedAt,
      stop: next.stop,
      ...(next.code === undefined ? {} : { code: next.code }),
    },
    next,
  );
}

function byteLength(value: string) {
  let bytes = 0;
  for (const character of value) {
    const point = character.codePointAt(0) ?? 0;
    bytes += point <= 0x7f ? 1 : point <= 0x7ff ? 2 : point <= 0xffff ? 3 : 4;
  }
  return bytes;
}

function correlationRequest(request: JonesOperationStartInput): JonesOperationStartInput {
  const action = request.action;
  return {
    operationId: request.operationId,
    environmentId: request.environmentId,
    expectedInstalledSource: request.expectedInstalledSource,
    action:
      action.kind === "download"
        ? { kind: "download", artifactId: action.artifactId, sourceSha: action.sourceSha }
        : action.kind === "download-exact"
          ? { kind: "download-exact", targetSource: action.targetSource }
          : {
              kind: "install",
              stagedHandle: action.stagedHandle,
              currentVersion: action.currentVersion,
              targetSource: action.targetSource,
              ...(action.continueRunningThreads === undefined
                ? {}
                : { continueRunningThreads: action.continueRunningThreads }),
              ...(action.recovery === undefined
                ? {}
                : {
                    recovery: {
                      policy: action.recovery.policy,
                      preflightId: action.recovery.preflightId,
                    },
                  }),
            },
    ...(request.correlation === undefined
      ? {}
      : {
          correlation: {
            planId: request.correlation.planId,
            deviceKey: request.correlation.deviceKey,
          },
        }),
  };
}
function correlationReceipt(receipt: JonesOperationReceipt): JonesOperationReceipt {
  return {
    ...correlationRequest(receipt),
    protocol: receipt.protocol,
    phase: receipt.phase,
    updatedAt: receipt.updatedAt,
    ...(receipt.acceptedAt === undefined ? {} : { acceptedAt: receipt.acceptedAt }),
    ...(receipt.jobId === undefined ? {} : { jobId: receipt.jobId }),
    ...(receipt.updateId === undefined ? {} : { updateId: receipt.updateId }),
    ...(receipt.code === undefined ? {} : { code: receipt.code }),
    ...(receipt.staged === undefined
      ? {}
      : {
          staged: {
            stagedHandle: receipt.staged.stagedHandle,
            sourceSha: receipt.staged.sourceSha,
            sourceTree: receipt.staged.sourceTree,
            artifactId: receipt.staged.artifactId,
            ...(receipt.staged.version === undefined ? {} : { version: receipt.staged.version }),
          },
        }),
    ...(receipt.outcome === undefined
      ? {}
      : {
          outcome: {
            status: receipt.outcome.status,
            fromVersion: receipt.outcome.fromVersion,
            targetVersion: receipt.outcome.targetVersion,
          },
        }),
    ...(receipt.recovery === undefined
      ? {}
      : {
          recovery: {
            requested: receipt.recovery.requested,
            ...(receipt.recovery.observed === undefined
              ? {}
              : { observed: receipt.recovery.observed }),
          },
        }),
    ...(receipt.stop === undefined
      ? {}
      : {
          stop: {
            stopId: receipt.stop.stopId,
            requestedAt: receipt.stop.requestedAt,
            ...(receipt.stop.result === undefined ? {} : { result: receipt.stop.result }),
          },
        }),
  };
}
function metadataDuty(duty: JonesActionDuty): JonesActionDuty {
  return {
    id: duty.id,
    environmentId: duty.environmentId,
    kind: duty.kind,
    action: duty.action,
    targetSource: duty.targetSource,
    ...(duty.stagedHandle === undefined ? {} : { stagedHandle: duty.stagedHandle }),
    ...(duty.currentVersion === undefined ? {} : { currentVersion: duty.currentVersion }),
    ...(duty.operationId === undefined ? {} : { operationId: duty.operationId }),
    ...(duty.stopId === undefined ? {} : { stopId: duty.stopId }),
    ...(duty.request === undefined ? {} : { request: correlationRequest(duty.request) }),
    ...(duty.receipt === undefined ? {} : { receipt: correlationReceipt(duty.receipt) }),
    ...(duty.native === undefined
      ? {}
      : {
          native: {
            commandId: duty.native.commandId,
            ...(duty.native.bodyHash === undefined ? {} : { bodyHash: duty.native.bodyHash }),
            requestType: duty.native.requestType,
            ...(duty.native.planId === undefined ? {} : { planId: duty.native.planId }),
            ...(duty.native.expectedRevision === undefined
              ? {}
              : { expectedRevision: duty.native.expectedRevision }),
          },
        }),
  };
}
function noExtraFields(value: unknown, metadata: unknown): boolean {
  if (value === null || typeof value !== "object") return true;
  if (metadata === null || typeof metadata !== "object") return false;
  return Object.keys(value).every(
    (key) =>
      Object.hasOwn(metadata, key) &&
      noExtraFields(
        (value as Record<string, unknown>)[key],
        (metadata as Record<string, unknown>)[key],
      ),
  );
}
function validDuty(duty: JonesActionDuty) {
  const target = (action: JonesOperationReceipt["action"]) =>
    action.kind === "download" ? action.sourceSha : action.targetSource;
  if (duty.kind === "operation")
    return (
      duty.request !== undefined &&
      duty.request.environmentId === duty.environmentId &&
      duty.request.operationId === duty.id &&
      duty.operationId === duty.id &&
      duty.action === duty.request.action.kind &&
      target(duty.request.action) === duty.targetSource &&
      (duty.receipt === undefined || validateReceipt(duty.request, duty.receipt))
    );
  if (duty.kind === "stop")
    return (
      duty.action === "stop" &&
      duty.receipt !== undefined &&
      duty.receipt.action.kind !== "install" &&
      duty.stopId === duty.id &&
      duty.operationId === duty.receipt.operationId &&
      duty.environmentId === duty.receipt.environmentId &&
      target(duty.receipt.action) === duty.targetSource &&
      validateReceipt(duty.receipt, duty.receipt)
    );
  return (
    duty.kind !== "native" ||
    (duty.native?.commandId === duty.id && duty.native.bodyHash !== undefined)
  );
}

/** A failed persistence operation holds admission; unresolved entries are never evicted. */
export function createJonesActionJournal(storage: JonesActionStorage) {
  let snapshot: {
    readonly ready: boolean;
    readonly fault?: string;
    readonly duties: readonly JonesActionDuty[];
    readonly history: readonly JonesActionDuty[];
  } = {
    ready: false,
    duties: [],
    history: [],
  };
  let hydration: Promise<void> | undefined;
  let queue: Promise<unknown> = Promise.resolve();
  const listeners = new Set<() => void>();
  const publish = (next: typeof snapshot) => {
    snapshot = next;
    for (const listener of listeners) listener();
  };
  const fail = () => {
    publish({
      ...snapshot,
      ready: true,
      fault: "Update request history is unavailable. No new work will be sent.",
    });
    throw new Error(snapshot.fault);
  };
  const read = async () => {
    try {
      const raw = await storage.getItem(JONES_ACTION_JOURNAL_KEY);
      if (raw !== null && byteLength(raw) > JONES_ACTION_JOURNAL_BYTES) return fail();
      const document = raw === null ? { schema: 1 as const, duties: [] } : JSON.parse(raw);
      if (!Schema.is(Document)(document)) return fail();
      const duties: readonly JonesActionDuty[] = document.duties;
      const history: readonly JonesActionDuty[] = document.history ?? [];
      const all = [...duties, ...history];
      if (
        !noExtraFields(document, {
          schema: 1,
          duties: duties.map(metadataDuty),
          history: history.map(metadataDuty),
        }) ||
        new Set(all.map((d) => d.id)).size !== all.length ||
        !all.every(validDuty)
      )
        return fail();
      return { ready: true, duties, history };
    } catch {
      fail();
    }
  };
  const load = () =>
    (hydration ??= (async () => {
      const next = await read();
      if (next !== undefined) publish(next);
    })());
  const mutate = (
    change: (duties: readonly JonesActionDuty[]) => readonly JonesActionDuty[],
    proof?: JonesOperationReceipt,
    archive = true,
  ) => {
    const changeUnderLock = async () => {
      await load();
      if (snapshot.fault !== undefined) throw new Error(snapshot.fault);
      if (storage.withLock !== undefined) {
        const persisted = await read();
        if (persisted === undefined) return fail();
        const identity = (d: JonesActionDuty) =>
          JSON.stringify([
            d.id,
            d.environmentId,
            d.kind,
            d.action,
            d.targetSource,
            d.stagedHandle,
            d.currentVersion,
            d.operationId,
            d.stopId,
            d.request,
            d.native,
          ]);
        if (
          snapshot.duties.some((d) => {
            const other = [...persisted.duties, ...persisted.history].find(
              (entry) => entry.id === d.id,
            );
            return (
              other === undefined ||
              identity(d) !== identity(other) ||
              (d.receipt !== undefined &&
                (other.receipt === undefined ||
                  !(d.kind === "stop"
                    ? validateJonesStopReceipt(d.receipt, other.receipt)
                    : validateReceipt(d.receipt, other.receipt))))
            );
          })
        )
          return fail();
        if (JSON.stringify(snapshot) !== JSON.stringify(persisted)) publish(persisted);
      }
      const duties = change(snapshot.duties);
      if (duties === snapshot.duties) return;
      const removed = (
        archive ? snapshot.duties.filter((d) => !duties.some((next) => next.id === d.id)) : []
      ).map((d) =>
        proof !== undefined &&
        d.operationId === proof.operationId &&
        d.environmentId === proof.environmentId
          ? { ...d, receipt: proof }
          : d,
      );
      const history = [...snapshot.history, ...removed].slice(-64);
      const document = { schema: 1 as const, duties, history };
      const json = JSON.stringify(document);
      if (
        !Schema.is(Document)(document) ||
        !duties.every(validDuty) ||
        byteLength(json) > JONES_ACTION_JOURNAL_BYTES
      )
        throw new Error("Update request history is full. Existing unknown requests were retained.");
      try {
        await storage.setItem(JONES_ACTION_JOURNAL_KEY, json);
      } catch {
        return fail();
      }
      publish({ ready: true, duties, history });
    };
    const work = queue.then(() => {
      if (storage.withLock === undefined) return changeUnderLock();
      let entered = false;
      return storage
        .withLock(async () => {
          entered = true;
          return changeUnderLock();
        })
        .catch((cause: unknown) => {
          if (!entered) return fail();
          throw cause;
        });
    });
    queue = work.catch(() => undefined);
    return work;
  };
  const acknowledge = (id: string) =>
    mutate((duties) =>
      duties.some((d) => d.id === id) ? duties.filter((d) => d.id !== id) : duties,
    );
  return {
    load,
    getSnapshot: () => snapshot,
    subscribe(listener: () => void) {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    reserve(duty: JonesActionDuty) {
      duty = metadataDuty(duty);
      return mutate((duties) => {
        const prior = [...duties, ...snapshot.history].find((d) => d.id === duty.id);
        if (prior !== undefined)
          throw new Error(
            "This request identity is already retained. Read its outcome; do not resend it.",
          );
        if (duties.length >= 64)
          throw new Error(
            "Update request history is full. Existing unknown requests were retained.",
          );
        if (
          duties.some(
            (d) =>
              d.environmentId === duty.environmentId && (duty.kind !== "stop" || d.kind === "stop"),
          )
        )
          throw new Error(
            "An earlier request for this environment still requires its exact outcome.",
          );
        return [...duties, duty];
      });
    },
    // Only the direct, acknowledged reply to this dispatch may use this path. Reads use proofs below.
    acknowledge,
    notSent: (id: string) =>
      mutate(
        (duties) => (duties.some((d) => d.id === id) ? duties.filter((d) => d.id !== id) : duties),
        undefined,
        false,
      ),
    async reconcileReceipt(receipt: JonesOperationReceipt) {
      if (!Schema.is(JonesOperationReceipt)(receipt)) return false;
      receipt = correlationReceipt(receipt);
      let settled = false;
      await mutate((duties) => {
        let changed = false;
        const next = duties.flatMap((d): JonesActionDuty[] => {
          if (d.environmentId !== receipt.environmentId || !["operation", "stop"].includes(d.kind))
            return [d];
          const original = d.request ?? d.receipt;
          if (
            original === undefined ||
            !(d.kind === "stop" && Schema.is(JonesOperationReceipt)(original)
              ? validateJonesStopReceipt(original, receipt)
              : validateReceipt(original, receipt)) ||
            (d.receipt !== undefined &&
              !(d.kind === "stop"
                ? validateJonesStopReceipt(d.receipt, receipt)
                : validateReceipt(d.receipt, receipt))) ||
            (d.kind === "stop" && receipt.stop?.stopId !== d.stopId)
          )
            return [d];
          changed = true;
          const stopSettled =
            d.kind !== "stop" ||
            ["stopped", "too-late", "not-stoppable-phase"].includes(receipt.stop?.result ?? "");
          if (settledJonesReceipt(receipt) && stopSettled) {
            settled = true;
            return [];
          }
          return [{ ...d, receipt }];
        });
        return changed ? next : duties;
      }, receipt);
      return settled;
    },
  };
}
export type JonesActionJournal = ReturnType<typeof createJonesActionJournal>;
