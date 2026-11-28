// @effect-diagnostics globalDate:off - Client reconciliation compares recorded receipt clocks; only target/native evidence supplies outcomes.
import * as Schema from "effect/Schema";
import { EnvironmentId } from "@t3tools/contracts";
import {
  JonesBoundOperationStartInput,
  JonesOperationId,
  JonesOperationStartInput,
  JonesOperationReceipt,
  JonesSourceSha,
} from "@t3tools/contracts/jones/jonesOperations";
import {
  isProvisionalUnsupportedReceipt,
  isSettledOperationReceipt,
  validateOperationEvidence,
  validateReceipt,
} from "../installPlan/model.ts";

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
export type JonesActionDuty = typeof JonesActionDuty.Type & {
  readonly unsupportedReceiptVerified?: true;
};
const Document = Schema.Struct({
  schema: Schema.Literal(1),
  duties: Schema.Array(JonesActionDuty).check(Schema.isMaxLength(64)),
  history: Schema.optionalKey(Schema.Array(JonesActionDuty).check(Schema.isMaxLength(64))),
});
export const JONES_ACTION_JOURNAL_KEY = "jones.update-duties.v1";
// Older clients retain these duties as holds. Only a client with the exact binding can settle them.
const BOUND_DUTIES_KEY = "jones.bound-action-duties.v1";
// Keep the v1 journal readable to existing clients. This optional proof only suppresses a repeat Read.
const VERIFIED_UNSUPPORTED_KEY = "jones.verified-unsupported.v1";
const JONES_ACTION_JOURNAL_BYTES = 128 * 1024;
const BOUND_DUTIES_BYTES = 2 * JONES_ACTION_JOURNAL_BYTES;
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
const stopResultSettled = (receipt: JonesOperationReceipt, stopId: string | undefined) =>
  receipt.stop !== undefined &&
  receipt.stop.stopId === stopId &&
  receipt.stop.result !== undefined &&
  receipt.stop.result !== "held";
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
  const admission = request.admission;
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
    ...(admission === undefined
      ? {}
      : {
          admission:
            admission.kind === "ordinary"
              ? { kind: "ordinary" }
              : {
                  kind: "follower",
                  enrollmentId: admission.enrollmentId,
                  directiveId: admission.directiveId,
                  targetSource: admission.targetSource,
                  targetTree: admission.targetTree,
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
export function sameRetainedJonesReceipt(
  previous: JonesOperationReceipt,
  next: JonesOperationReceipt,
): boolean {
  return (
    canonicalJonesCommand(correlationReceipt(previous)) ===
    canonicalJonesCommand(correlationReceipt(next))
  );
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
function hasBoundAdmission(duty: JonesActionDuty) {
  return duty.request?.admission !== undefined || duty.receipt?.admission !== undefined;
}
function legacyProjection(duty: JonesActionDuty): JonesActionDuty {
  const metadata = metadataDuty(duty);
  if (!hasBoundAdmission(metadata)) return metadata;
  const { request: _request, receipt: _receipt, ...hold } = metadata;
  return { ...hold, kind: "legacy" };
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
      (duty.request.admission === undefined || isBoundRequest(duty.request)) &&
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
const compiledIs1 = Schema.is(Document);
const compiledIs2 = Schema.is(JonesOperationReceipt);
const isBoundDutyList = Schema.is(Schema.Array(JonesActionDuty).check(Schema.isMaxLength(256)));
const isVerifiedUnsupportedList = Schema.is(
  Schema.Array(JonesOperationReceipt).check(Schema.isMaxLength(64)),
);

function verifiedUnsupportedReceipts(history: readonly JonesActionDuty[]) {
  return history.flatMap((d) =>
    d.unsupportedReceiptVerified === true && d.receipt !== undefined ? [d.receipt] : [],
  );
}
const isBoundRequest = Schema.is(JonesBoundOperationStartInput);

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
      if (!compiledIs1(document)) return fail();
      let duties: readonly JonesActionDuty[] = document.duties;
      let history: readonly JonesActionDuty[] = document.history ?? [];
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
      if (all.some((d) => d.kind === "legacy" && d.operationId !== undefined)) {
        const bindings = await storage.getItem(BOUND_DUTIES_KEY);
        if (bindings !== null) {
          if (byteLength(bindings) > BOUND_DUTIES_BYTES) return fail();
          const decoded: unknown = JSON.parse(bindings);
          if (!isBoundDutyList(decoded)) return fail();
          const byId = new Map(decoded.map((d) => [d.id, d]));
          if (
            byId.size !== decoded.length ||
            !decoded.every(
              (d) => hasBoundAdmission(d) && noExtraFields(d, metadataDuty(d)) && validDuty(d),
            )
          )
            return fail();
          const restore = (d: JonesActionDuty): JonesActionDuty => {
            const bound = byId.get(d.id);
            return d.kind === "legacy" &&
              bound !== undefined &&
              canonicalJonesCommand(d) === canonicalJonesCommand(legacyProjection(bound))
              ? bound
              : d;
          };
          duties = duties.map(restore);
          history = history.map(restore);
        }
      }
      if (
        !history.some(
          (d) =>
            d.kind === "operation" &&
            d.receipt !== undefined &&
            isProvisionalUnsupportedReceipt(d.receipt),
        )
      )
        return { ready: true, duties, history };
      let proof: readonly JonesOperationReceipt[] = [];
      try {
        const saved = await storage.getItem(VERIFIED_UNSUPPORTED_KEY);
        if (saved !== null && byteLength(saved) <= JONES_ACTION_JOURNAL_BYTES) {
          const decoded: unknown = JSON.parse(saved);
          if (isVerifiedUnsupportedList(decoded)) proof = decoded;
        }
      } catch {
        // The original journal remains available when optional Read evidence is unavailable.
      }
      return {
        ready: true,
        duties,
        history: history.map((d): JonesActionDuty => {
          const receipt = d.receipt;
          return d.kind === "operation" &&
            receipt !== undefined &&
            isProvisionalUnsupportedReceipt(receipt) &&
            proof.some((candidate) => sameRetainedJonesReceipt(receipt, candidate))
            ? { ...d, unsupportedReceiptVerified: true }
            : d;
        }),
      };
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
    evidence?: { readonly receipt: JonesOperationReceipt; readonly ledgerRead?: boolean },
    archive = true,
  ) => {
    const changeUnderLock = async () => {
      await load();
      if (snapshot.fault !== undefined) throw new Error(snapshot.fault);
      const proof = evidence?.receipt;
      if (storage.withLock !== undefined) {
        const persisted = await read();
        if (persisted === undefined) return fail();
        const identity = (d: JonesActionDuty) =>
          canonicalJonesCommand([
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
                    : validateReceipt(d.receipt, other.receipt, {
                        ledgerRead:
                          evidence?.ledgerRead === true &&
                          canonicalJonesCommand(other.receipt) === canonicalJonesCommand(proof),
                      }))))
            );
          })
        )
          return fail();
        if (JSON.stringify(snapshot) !== JSON.stringify(persisted)) publish(persisted);
      }
      const updatedDuties = change(snapshot.duties);
      const recovered: JonesActionDuty[] = [];
      const previousHistory = snapshot.history;
      const revisedHistory =
        evidence?.ledgerRead !== true || proof === undefined
          ? previousHistory
          : previousHistory.flatMap((d) => {
              if (
                d.kind === "stop" &&
                d.receipt !== undefined &&
                !stopResultSettled(d.receipt, d.stopId) &&
                stopResultSettled(proof, d.stopId) &&
                isSettledOperationReceipt(proof) &&
                validateJonesStopReceipt(d.receipt, proof)
              )
                return [{ ...d, receipt: proof }];
              if (
                d.kind !== "operation" ||
                d.request === undefined ||
                d.receipt === undefined ||
                !isProvisionalUnsupportedReceipt(d.receipt) ||
                d.unsupportedReceiptVerified === true ||
                !validateOperationEvidence(d.request, proof, { ledgerRead: true }) ||
                !validateOperationEvidence(d.receipt, proof, { ledgerRead: true })
              )
                return [d];
              if (isSettledOperationReceipt(proof))
                return [
                  {
                    ...d,
                    receipt: proof,
                    ...(isProvisionalUnsupportedReceipt(proof)
                      ? { unsupportedReceiptVerified: true as const }
                      : {}),
                  },
                ];
              // Keep live work in the duty journal so later receipts and exact Stop can settle it.
              recovered.push({ ...d, receipt: proof });
              return [];
            });
      const duties = recovered.length === 0 ? updatedDuties : [...updatedDuties, ...recovered];
      if (duties === snapshot.duties && revisedHistory.every((d, i) => d === previousHistory[i]))
        return;
      const removed = (
        archive ? snapshot.duties.filter((d) => !duties.some((next) => next.id === d.id)) : []
      ).map((d) =>
        proof !== undefined &&
        d.operationId === proof.operationId &&
        d.environmentId === proof.environmentId
          ? {
              ...d,
              receipt: proof,
              ...(d.kind === "operation" &&
              evidence?.ledgerRead === true &&
              isProvisionalUnsupportedReceipt(proof)
                ? { unsupportedReceiptVerified: true as const }
                : {}),
            }
          : d,
      );
      let history = [...revisedHistory, ...removed].slice(-64);
      const document = {
        schema: 1 as const,
        duties: duties.map(metadataDuty),
        history: history.map(metadataDuty),
      };
      if (
        !compiledIs1(document) ||
        !duties.every(validDuty) ||
        !history.every(validDuty) ||
        byteLength(JSON.stringify(document)) > JONES_ACTION_JOURNAL_BYTES
      )
        throw new Error("Update request history is full. Existing unknown requests were retained.");
      const previousBound = [...snapshot.duties, ...previousHistory].filter(hasBoundAdmission);
      const nextBound = [...document.duties, ...document.history].filter(hasBoundAdmission);
      const json = JSON.stringify({
        schema: 1,
        duties: document.duties.map(legacyProjection),
        history: document.history.map(legacyProjection),
      });
      try {
        if (previousBound.length > 0 || nextBound.length > 0) {
          // An interrupted write leaves the old v1 references readable. The v1 document
          // decides which sidecar records belong to a duty and whether it is still pending.
          const combined = new Map(
            [...previousBound, ...nextBound].map((d) => [d.id, metadataDuty(d)]),
          );
          const serialized = JSON.stringify([...combined.values()]);
          if (byteLength(serialized) > BOUND_DUTIES_BYTES) return fail();
          await storage.setItem(BOUND_DUTIES_KEY, serialized);
        }
        await storage.setItem(JONES_ACTION_JOURNAL_KEY, json);
      } catch {
        return fail();
      }
      const previousProof = JSON.stringify(verifiedUnsupportedReceipts(previousHistory));
      const nextProof = JSON.stringify(verifiedUnsupportedReceipts(history));
      if (previousProof !== nextProof) {
        try {
          await storage.setItem(VERIFIED_UNSUPPORTED_KEY, nextProof);
        } catch {
          // Keep the compatible journal result. A later exact Read can recover confirmation.
          history = document.history;
        }
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
              d.environmentId === duty.environmentId &&
              (duty.kind !== "stop" || d.action === "stop"),
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
    async reconcileReceipt(
      receipt: JonesOperationReceipt,
      options: { readonly ledgerRead?: boolean } = {},
    ) {
      if (!validateReceipt(receipt, receipt)) return false;
      receipt = correlationReceipt(receipt);
      let settled = false;
      await mutate(
        (duties) => {
          let changed = false;
          const next = duties.flatMap((d): JonesActionDuty[] => {
            if (
              d.environmentId !== receipt.environmentId ||
              !["operation", "stop"].includes(d.kind)
            )
              return [d];
            const original = d.request ?? d.receipt;
            const terminalRead = options.ledgerRead === true && isSettledOperationReceipt(receipt);
            if (
              original === undefined ||
              !(d.kind === "stop" && compiledIs2(original)
                ? validateJonesStopReceipt(original, receipt)
                : validateOperationEvidence(original, receipt, options)) ||
              (d.receipt !== undefined &&
                !(d.kind === "stop"
                  ? validateJonesStopReceipt(d.receipt, receipt)
                  : validateOperationEvidence(d.receipt, receipt, options))) ||
              (d.kind === "stop" && receipt.stop?.stopId !== d.stopId && !terminalRead)
            )
              return [d];
            changed = true;
            const stopSettled =
              d.kind !== "stop" || terminalRead || stopResultSettled(receipt, d.stopId);
            if (isSettledOperationReceipt(receipt) && stopSettled) {
              settled = true;
              return [];
            }
            return [{ ...d, receipt }];
          });
          return changed ? next : duties;
        },
        { receipt, ...options },
      );
      return settled;
    },
  };
}
export type JonesActionJournal = ReturnType<typeof createJonesActionJournal>;
