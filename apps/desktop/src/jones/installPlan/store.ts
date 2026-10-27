// @effect-diagnostics nodeBuiltinImport:off - Durable native plan CAS storage, outside rollback-restored profile data.
import * as NodeCrypto from "node:crypto";
import * as NodePath from "node:path";
import * as Schema from "effect/Schema";
import {
  JonesInstallPlan,
  JonesInstallPlanCommandReceipt,
  type JonesInstallPlanState,
} from "@t3tools/contracts/jones/installPlan";
import { JonesOperationId, JonesSourceSha } from "@t3tools/contracts/jones/jonesOperations";
import {
  ownedDirectory,
  readPrivateJson,
  replacePrivateJson,
  withPrivateJournalLock,
} from "../storage/privateJsonStore.ts";

export const PLAN_BYTES = 1024 * 1024;
const Document = Schema.Struct({
  schema: Schema.Literal(1),
  current: Schema.NullOr(JonesInstallPlan),
  last: Schema.NullOr(JonesInstallPlan),
  commands: Schema.optionalKey(
    Schema.Array(
      Schema.Struct({
        body: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
        receipt: JonesInstallPlanCommandReceipt,
      }),
    ).check(Schema.isMaxLength(64)),
  ),
});
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
  ).check(Schema.isMaxLength(64)),
});
export type PlanDocument = typeof Document.Type;
const validDocument = (value: unknown): value is PlanDocument =>
  Schema.is(Document)(value) &&
  new Set(value.commands?.map((entry) => entry.receipt.command.commandId)).size ===
    (value.commands?.length ?? 0) &&
  (value.commands ?? []).every(
    (entry) => entry.receipt.bodyHash === undefined || entry.receipt.bodyHash === entry.body,
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
const newer = (value: unknown) =>
  typeof value === "object" &&
  value !== null &&
  "schema" in value &&
  typeof value.schema === "number" &&
  value.schema > 1;

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
            const document = await readPrivateJson(
              NodePath.join(directory, "plan.json"),
              PLAN_BYTES,
            );
            const dispositions = await readPrivateJson(
              NodePath.join(directory, "dispositions.json"),
              PLAN_BYTES,
            );
            hasDispositions = dispositions !== null;
            if (newer(document) || newer(dispositions))
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
              (dispositions !== null && !Schema.is(Dispositions)(dispositions))
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
          if (
            !validDocument(next.document) ||
            !Schema.is(Dispositions)(next.dispositions) ||
            Buffer.byteLength(JSON.stringify(next.document)) + 1 > PLAN_BYTES ||
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
            await replacePrivateJson(
              NodePath.join(directory, "plan.json"),
              next.document,
              PLAN_BYTES,
            );
          return next.result;
        });
      };
      const result = queue.then(run);
      queue = result.catch(() => undefined);
      return result;
    },
  };
}
