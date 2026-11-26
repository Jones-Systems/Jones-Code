import { TaskFolder, TaskFolderError, TaskFolderRepository } from "@t3tools/contracts";
import { importedHistoryCanonicalJson as canonical } from "@t3tools/shared/jones/importedHistoryCanonical";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/sql/SqlClient";
import type * as SqlError from "effect/sql/SqlError";

import { DirectoryIdentity } from "../ordinaryCheckout/CheckoutProtocol.ts";
import { makeJonesTablePresence } from "../persistence/JonesTablePresence.ts";

const FolderOwner = Schema.Struct({
  folder: TaskFolder,
  createdAt: Schema.NonEmptyString,
  realRoot: Schema.NonEmptyString,
});
export type FolderOwner = typeof FolderOwner.Type;

const FolderRow = Schema.Struct({
  ...TaskFolder.fields,
  createdAt: FolderOwner.fields.createdAt,
  realRoot: FolderOwner.fields.realRoot,
  projectIsWorktree: Schema.BooleanFromBit,
  repositories: Schema.fromJsonString(Schema.Array(TaskFolderRepository)),
});
const decodeFolder = Schema.decodeUnknownEffect(FolderRow);

const Directory = Schema.Struct({
  canonicalPath: Schema.NonEmptyString,
  directoryIdentity: DirectoryIdentity,
});
export type Directory = typeof Directory.Type;

const Checkout = Schema.Struct({
  worktreePath: Schema.NonEmptyString,
  repositoryPath: Schema.NonEmptyString,
  directory: Directory,
  repository: Directory,
  admin: Directory,
  common: Directory,
  head: Schema.NonEmptyString,
});
export type Checkout = typeof Checkout.Type;

const Capture = Schema.Struct({
  version: Schema.Literal(1),
  owner: FolderOwner,
  ownerThreadId: Schema.NullOr(Schema.NonEmptyString),
  projectCreatedAt: Schema.NonEmptyString,
  root: Directory,
  projectRoot: Directory,
  projectPath: Directory,
  checkouts: Schema.Array(Checkout),
  commandHash: Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/)),
  shell: Schema.NonEmptyString,
  branch: Schema.String,
});
export type Capture = typeof Capture.Type;

const Receipt = Schema.Struct({
  root: Schema.String,
  realRoot: Schema.String,
  claimId: Schema.String,
  outcome: Schema.Literals(["unknown", "succeeded"]),
  capture: Schema.fromJsonString(Capture),
});
const decodeReceipt = Schema.decodeUnknownEffect(Receipt);
const encodeCapture = Schema.encodeEffect(Schema.fromJsonString(Capture));

const failure = (detail: string) => new TaskFolderError({ operation: "remove", detail });
const isTaskFolderError = Schema.is(TaskFolderError);
const mapFailure = (cause: unknown) =>
  isTaskFolderError(cause)
    ? cause
    : new TaskFolderError({
        operation: "remove",
        detail: "Could not verify the durable layout teardown receipt.",
        cause,
      });

export const ownerMatches = (current: FolderOwner | null, original: FolderOwner) => {
  if (current === null) return false;
  const { repositories: remaining, ...folder } = current.folder;
  const { repositories: captured, ...originalFolder } = original.folder;
  const indexes = remaining.map((member) =>
    captured.findIndex((entry) => canonical(entry) === canonical(member)),
  );
  return (
    canonical({ ...current, folder }) === canonical({ ...original, folder: originalFolder }) &&
    indexes.every((index, i) => index > (indexes[i - 1] ?? -1))
  );
};

export const make = (sql: SqlClient.SqlClient) => {
  const available = makeJonesTablePresence(sql)("jones_task_folder_teardowns").pipe(
    Effect.mapError(mapFailure),
  );
  const readOwner = Effect.fn("TaskFolderTeardown.readOwner")(function* (root: string) {
    const rows = yield* sql`
      SELECT root, real_root AS "realRoot", created_at AS "createdAt", project_id AS "projectId",
        project_root AS "projectRoot", project_path AS "projectPath",
        project_is_worktree AS "projectIsWorktree", anchor, repositories_json AS "repositories"
      FROM task_folders WHERE root = ${root}
    `;
    if (rows[0] === undefined) return null;
    const { realRoot, createdAt, ...folder } = yield* decodeFolder(rows[0]);
    return { folder, realRoot, createdAt } satisfies FolderOwner;
  }, Effect.mapError(mapFailure));
  const read = Effect.fn("TaskFolderTeardown.readReceipt")(function* (owner: FolderOwner) {
    if (!(yield* available)) return null;
    const rows = yield* sql`
      SELECT root, real_root AS "realRoot", claim_id AS "claimId", outcome, capture_json AS capture
      FROM jones_task_folder_teardowns
      WHERE root = ${owner.folder.root} OR real_root = ${owner.realRoot}
    `;
    if (rows.length > 1)
      return yield* failure("Conflicting layout teardown receipts need reconciliation.");
    if (rows[0] === undefined) return null;
    const receipt = yield* decodeReceipt(rows[0]);
    if (
      receipt.root !== receipt.capture.owner.folder.root ||
      receipt.realRoot !== receipt.capture.owner.realRoot
    )
      return yield* failure("The layout teardown receipt owner changed.");
    return receipt;
  }, Effect.mapError(mapFailure));

  return {
    available,
    readOwner,
    read,
    claim: Effect.fn("TaskFolderTeardown.claim")(function* (capture: Capture) {
      // A savepoint would not commit the receipt before the command runs.
      if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
        return yield* failure("Layout teardown needs a committed receipt before it can run.");
      if (!(yield* available))
        return yield* failure("Layout teardown receipt storage is unavailable.");
      const { owner } = capture;
      const encoded = yield* encodeCapture(capture);
      return yield* sql.withTransaction(
        Effect.gen(function* () {
          if (canonical(yield* readOwner(owner.folder.root)) !== canonical(owner))
            return yield* failure("The task folder owner changed before layout teardown.");
          const rows = yield* sql<{ claimId: string }>`
            INSERT INTO jones_task_folder_teardowns (root, real_root, capture_json, outcome)
            VALUES (${owner.folder.root}, ${owner.realRoot}, ${encoded}, 'unknown')
            ON CONFLICT DO NOTHING RETURNING claim_id AS "claimId"
          `;
          const row = rows[0];
          if (row === undefined)
            return yield* failure("A layout teardown receipt already exists for this folder.");
          return row.claimId;
        }),
      );
    }, Effect.mapError(mapFailure)),
    succeed: Effect.fn("TaskFolderTeardown.recordSuccess")(function* (
      root: string,
      claimId: string,
    ) {
      const rows = yield* sql`
        UPDATE jones_task_folder_teardowns SET outcome = 'succeeded'
        WHERE root = ${root} AND claim_id = ${claimId} AND outcome = 'unknown'
        RETURNING root
      `;
      if (rows.length !== 1) return yield* failure("The layout teardown receipt claim changed.");
    }, Effect.mapError(mapFailure)),
    forget: Effect.fn("TaskFolderTeardown.forget")(function* (
      root: string,
      removeFolder: Effect.Effect<void, SqlError.SqlError>,
    ) {
      if (!(yield* available)) return yield* removeFolder;
      yield* sql.withTransaction(
        Effect.gen(function* () {
          const owner = yield* readOwner(root);
          if (owner === null) return;
          const receipt = yield* read(owner);
          if (receipt !== null) {
            if (receipt.outcome !== "succeeded" || !ownerMatches(owner, receipt.capture.owner))
              return yield* failure(
                "Layout teardown must be reconciled before forgetting the folder.",
              );
            yield* sql`
              DELETE FROM jones_task_folder_teardowns
              WHERE root = ${root} AND claim_id = ${receipt.claimId} AND outcome = 'succeeded'
            `;
          }
          yield* removeFolder;
        }),
      );
    }, Effect.mapError(mapFailure)),
  };
};
export type ReceiptStore = ReturnType<typeof make>;
