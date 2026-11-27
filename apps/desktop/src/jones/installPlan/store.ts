// @effect-diagnostics nodeBuiltinImport:off - Durable native plan CAS storage, outside rollback-restored profile data.
import * as NodeCrypto from "node:crypto";
import * as NodePath from "node:path";
import * as Schema from "effect/Schema";
import {
  JonesInstallPlan,
  JonesInstallPlanCommandReceipt,
  JONES_INSTALL_PLAN_HISTORY_LIMIT,
  type JonesInstallPlanState,
} from "@t3tools/contracts/jones/installPlan";
import {
  JonesOperationId,
  JonesOperationReceipt,
  JonesSourceSha,
} from "@t3tools/contracts/jones/jonesOperations";
import {
  ownedDirectory,
  readPrivateJson,
  replacePrivateJson,
  withPrivateJournalLock,
} from "../storage/privateJsonStore.ts";

const PLAN_BYTES = 1024 * 1024;
// V1 permits two plans with 16 devices and 16 operations per device. Even if all 512
// operations still learn a receipt, V2 has room for their bounded receipt and recovery
// fields plus the repeated original identities. New admissions use a much smaller budget.
const RECOVERY_DOCUMENT_BYTES = 20 * 1024 * 1024;
// Keep replay identities and publication dispositions even after old plan detail ages out.
export const MAX_PLAN_COMMANDS = 1024;
export const MAX_PLAN_DISPOSITIONS = 2048;
const slots = {
  current: Schema.NullOr(JonesInstallPlan),
  last: Schema.NullOr(JonesInstallPlan),
};
const Commands = Schema.Array(
  Schema.Struct({
    body: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
    receipt: JonesInstallPlanCommandReceipt,
    orphanReceipt: Schema.optionalKey(JonesOperationReceipt),
  }),
);
const LegacyDocument = Schema.Struct({
  schema: Schema.Literal(1),
  ...slots,
  commands: Schema.optionalKey(Commands.check(Schema.isMaxLength(64))),
});
const RecoveryDocument = Schema.Struct({
  schema: Schema.Literal(2),
  ...slots,
  documentRevision: Schema.Int.check(Schema.isGreaterThan(0)),
  older: Schema.Array(JonesInstallPlan).check(
    Schema.isMaxLength(JONES_INSTALL_PLAN_HISTORY_LIMIT - 1),
  ),
  commands: Schema.optionalKey(Commands.check(Schema.isMaxLength(MAX_PLAN_COMMANDS))),
});
const Document = Schema.Union([LegacyDocument, RecoveryDocument]);
const Dispositions = Schema.Struct({
  schema: Schema.Literal(1),
  targets: Schema.Array(
    Schema.Struct({
      targetSource: JonesSourceSha,
      targetTree: JonesSourceSha,
      planId: JonesOperationId,
      createdAt: Schema.String,
      laptopStopped: Schema.optionalKey(Schema.Boolean),
    }),
  ).check(Schema.isMaxLength(MAX_PLAN_DISPOSITIONS)),
});
export type PlanDocument = typeof Document.Type;
const isDocument = Schema.is(Document);

export const closedPlans = (document: PlanDocument) => [
  ...(document.last === null ? [] : [document.last]),
  ...(document.schema === 2 ? document.older : []),
];
export const retainedPlans = (document: PlanDocument) => [
  ...(document.current === null ? [] : [document.current]),
  ...closedPlans(document),
];

const validDocument = (value: unknown): value is PlanDocument =>
  isDocument(value) &&
  (value.schema === 1 ||
    ((value.last !== null || value.older.length === 0) &&
      new Set(retainedPlans(value).map((plan) => plan.planId)).size ===
        retainedPlans(value).length)) &&
  retainedPlans(value).every(
    (plan) =>
      plan?.laptop.gatePause !== true ||
      (plan.laptop.gate !== undefined && plan.laptop.gateClosed?.reason === "blocked"),
  ) &&
  new Set(value.commands?.map((entry) => entry.receipt.command.commandId)).size ===
    (value.commands?.length ?? 0) &&
  (value.commands ?? []).every(
    ({ body, receipt, orphanReceipt }) =>
      (receipt.bodyHash === undefined || receipt.bodyHash === body) &&
      (orphanReceipt === undefined ||
        (value.schema === 2 &&
          receipt.outcome === "terminal" &&
          receipt.requestType === "claim" &&
          receipt.operationId === orphanReceipt.operationId &&
          receipt.command.environmentId === orphanReceipt.environmentId &&
          receipt.planId === orphanReceipt.correlation?.planId)),
  );
export type PlanDispositions = typeof Dispositions.Type;
export interface PlanSnapshot {
  readonly document: PlanDocument;
  readonly dispositions: PlanDispositions;
  readonly readOnly?: JonesInstallPlanState["readOnly"];
}
export interface InstallPlanStore {
  readonly transact: <T>(
    work: (snapshot: PlanSnapshot) => Promise<{
      readonly document: PlanDocument;
      readonly dispositions: PlanDispositions;
      readonly result: T;
    }>,
  ) => Promise<T>;
}
export class PlanStorageError extends Error {
  readonly code: "bounds" | "storage";
  constructor(code: "bounds" | "storage", message: string) {
    super(message);
    this.code = code;
  }
}
const empty = (): PlanSnapshot => ({
  document: { schema: 1, current: null, last: null },
  dispositions: { schema: 1, targets: [] },
});
const schemaAbove = (value: unknown, supported: number) =>
  typeof value === "object" &&
  value !== null &&
  "schema" in value &&
  typeof value.schema === "number" &&
  value.schema > supported;

const isDispositions = Schema.is(Dispositions);

async function readDocument(file: string): Promise<unknown> {
  try {
    return await readPrivateJson(file, PLAN_BYTES);
  } catch (cause) {
    const larger = await readPrivateJson(file, RECOVERY_DOCUMENT_BYTES);
    if (!schemaAbove(larger, 1)) throw cause;
    return larger;
  }
}

export function createInstallPlanStore(options: {
  readonly home: string;
  readonly profile: string | undefined;
}): InstallPlanStore {
  const runtime = NodePath.join(options.home, "runtime");
  const parent = NodePath.join(runtime, "jones-install-plan");
  const directory = NodePath.join(
    parent,
    `desktop-${NodeCrypto.createHash("sha256")
      .update(options.profile ?? "")
      .digest("hex")}`,
  );
  let queue: Promise<unknown> = Promise.resolve();
  return {
    transact: (work) => {
      const run = async () => {
        await ownedDirectory(runtime, false);
        for (const path of [parent, directory]) await ownedDirectory(path);
        return withPrivateJournalLock(directory, async () => {
          let snapshot = empty();
          let hasDispositions = false;
          try {
            const document = await readDocument(NodePath.join(directory, "plan.json"));
            const dispositions = await readPrivateJson(
              NodePath.join(directory, "dispositions.json"),
              PLAN_BYTES,
            );
            hasDispositions = dispositions !== null;
            if (schemaAbove(document, 2) || schemaAbove(dispositions, 1))
              snapshot = {
                ...snapshot,
                readOnly: {
                  code: "newer-schema",
                  message: "A newer desktop owns this plan. Its files were retained.",
                },
              };
            else if (
              (document !== null && dispositions === null) ||
              (document !== null && !validDocument(document)) ||
              (dispositions !== null && !isDispositions(dispositions))
            )
              snapshot = {
                ...snapshot,
                readOnly: {
                  code: "invalid",
                  message: "The plan journal is invalid. Its files were retained.",
                },
              };
            else
              snapshot = {
                document: document === null ? snapshot.document : (document as PlanDocument),
                dispositions:
                  dispositions === null
                    ? snapshot.dispositions
                    : (dispositions as PlanDispositions),
              };
          } catch {
            snapshot = {
              ...snapshot,
              readOnly: {
                code: "invalid",
                message: "The plan journal could not be qualified. Its files were retained.",
              },
            };
          }
          const next = await work(snapshot);
          const changed =
            next.document !== snapshot.document || next.dispositions !== snapshot.dispositions;
          if (snapshot.readOnly !== undefined && changed)
            throw new PlanStorageError("storage", "Read-only plan evidence cannot be replaced.");
          if (!changed) return next.result;
          const limit = next.document.schema === 2 ? RECOVERY_DOCUMENT_BYTES : PLAN_BYTES;
          if (
            !validDocument(next.document) ||
            !isDispositions(next.dispositions) ||
            Buffer.byteLength(JSON.stringify(next.document)) + 1 > limit ||
            Buffer.byteLength(JSON.stringify(next.dispositions)) + 1 > PLAN_BYTES
          )
            throw new PlanStorageError("bounds", "The plan reached its retained storage bound.");
          // A crash between these writes conservatively withholds publication of the target.
          if (
            next.dispositions !== snapshot.dispositions ||
            (!hasDispositions && next.document !== snapshot.document)
          )
            await replacePrivateJson(
              NodePath.join(directory, "dispositions.json"),
              next.dispositions,
              PLAN_BYTES,
            );
          if (next.document !== snapshot.document)
            await replacePrivateJson(NodePath.join(directory, "plan.json"), next.document, limit);
          return next.result;
        });
      };
      const result = queue.then(run);
      queue = result.catch(() => undefined);
      return result;
    },
  };
}
