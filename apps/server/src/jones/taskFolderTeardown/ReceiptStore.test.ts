import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { ProjectId } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as SqlClient from "effect/sql/SqlClient";

import taskFoldersMigration from "../../persistence/Migrations/105_TaskFolders.ts";
import migration from "../persistence/Migrations/166_JonesTaskFolderTeardown.ts";
import * as Receipts from "./ReceiptStore.ts";

const fixtureOwner = {
  createdAt: "2026-11-26T00:00:00.000Z",
  realRoot: "/fixtures/work",
  folder: {
    root: "/fixtures/alias/work",
    projectId: ProjectId.make("project:teardown-fixture"),
    projectRoot: "/fixtures/project",
    anchor: "/fixtures",
    projectPath: "/fixtures/alias/work/project",
    projectIsWorktree: true,
    repositories: [],
  },
} satisfies Receipts.FolderOwner;

const directory = (canonicalPath: string, inode: string) => ({
  canonicalPath,
  directoryIdentity: { version: 1 as const, device: "1", inode, birth: "1" },
});
const capture = {
  version: 1,
  owner: fixtureOwner,
  ownerThreadId: null,
  projectCreatedAt: fixtureOwner.createdAt,
  root: directory(fixtureOwner.realRoot, "2"),
  projectRoot: directory(fixtureOwner.folder.projectRoot, "3"),
  projectPath: directory("/fixtures/work/project", "4"),
  checkouts: [
    {
      worktreePath: fixtureOwner.folder.projectPath,
      repositoryPath: fixtureOwner.folder.projectRoot,
      directory: directory("/fixtures/work/project", "4"),
      repository: directory(fixtureOwner.folder.projectRoot, "3"),
      admin: directory("/fixtures/project/.git/worktrees/project", "5"),
      common: directory("/fixtures/project/.git", "6"),
      head: "ref: refs/heads/fixture",
    },
  ],
  commandHash: "a".repeat(64),
  shell: "sh",
  branch: "",
} satisfies Receipts.Capture;

const insert = Effect.fnUntraced(function* ({ folder, realRoot, createdAt }: Receipts.FolderOwner) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    INSERT INTO task_folders (
      root, real_root, project_id, project_root, anchor, project_path, project_is_worktree, created_at
    ) VALUES (
      ${folder.root}, ${realRoot}, ${folder.projectId}, ${folder.projectRoot}, ${folder.anchor},
      ${folder.projectPath}, ${folder.projectIsWorktree ? 1 : 0}, ${createdAt}
    )
  `;
});

it.live("commits a single claim, retains it on reopen, and forgets only proven success", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const dir = yield* fileSystem.makeTempDirectoryScoped({ prefix: "task-folder-teardown-" });
    const filename = path.join(dir, "test.sqlite");
    const claimId = yield* Effect.gen(function* () {
      yield* taskFoldersMigration;
      yield* migration;
      yield* insert(fixtureOwner);
      const sql = yield* SqlClient.SqlClient;
      const store = Receipts.make(sql);
      const nested = yield* sql.withTransaction(store.claim(capture)).pipe(Effect.result);
      assert.equal(nested._tag, "Failure");
      assert.isNull(yield* store.read(fixtureOwner));
      const contenders = yield* Effect.all(
        [store.claim(capture).pipe(Effect.result), store.claim(capture).pipe(Effect.result)],
        { concurrency: 2 },
      );
      const wins = contenders.flatMap((result) =>
        result._tag === "Success" ? [result.success] : [],
      );
      assert.lengthOf(wins, 1);
      return wins[0]!;
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename })));

    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const store = Receipts.make(sql);
      const root = fixtureOwner.folder.root;
      const removeFolder = sql`DELETE FROM task_folders WHERE root = ${root}`.pipe(Effect.asVoid);
      const reloaded = yield* store.read(fixtureOwner);
      assert.equal(reloaded?.outcome, "unknown");
      assert.deepEqual(reloaded?.capture, capture);
      assert.equal((yield* store.forget(root, removeFolder).pipe(Effect.result))._tag, "Failure");
      assert.deepEqual(yield* store.readOwner(root), fixtureOwner);

      const alias = {
        ...fixtureOwner,
        folder: { ...fixtureOwner.folder, root: fixtureOwner.realRoot },
      };
      yield* insert(alias);
      assert.equal((yield* store.read(alias))?.outcome, "unknown");
      assert.equal(
        (yield* store.claim({ ...capture, owner: alias }).pipe(Effect.result))._tag,
        "Failure",
      );
      assert.equal(
        (yield* store.succeed(root, "another-claim").pipe(Effect.result))._tag,
        "Failure",
      );
      yield* store.succeed(root, claimId);
      assert.equal((yield* store.read(fixtureOwner))?.outcome, "succeeded");
      yield* sql`UPDATE task_folders SET created_at = 'replacement' WHERE root = ${root}`;
      assert.equal((yield* store.forget(root, removeFolder).pipe(Effect.result))._tag, "Failure");
      yield* sql`UPDATE task_folders SET created_at = ${fixtureOwner.createdAt} WHERE root = ${root}`;
      yield* store.forget(root, removeFolder);
      assert.isNull(yield* store.readOwner(root));
      assert.isNull(yield* store.read(alias));
      yield* insert(fixtureOwner);
      assert.notEqual(yield* store.claim(capture), claimId);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename })));
  }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
);

it.effect("holds a proposed claim while the Jones migration is unavailable", () =>
  Effect.gen(function* () {
    yield* taskFoldersMigration;
    yield* insert(fixtureOwner);
    const sql = yield* SqlClient.SqlClient;
    const store = Receipts.make(sql);
    assert.isFalse(yield* store.available);
    assert.equal((yield* store.claim(capture).pipe(Effect.result))._tag, "Failure");
    assert.deepEqual(yield* store.readOwner(fixtureOwner.folder.root), fixtureOwner);
    yield* migration;
    assert.isTrue(yield* store.available);
    assert.isString(yield* store.claim(capture));
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);
