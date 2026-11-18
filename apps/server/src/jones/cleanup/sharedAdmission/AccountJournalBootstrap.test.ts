import { assert, describe, it } from "@effect/vitest";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- The independent-process race needs fork IPC and the exact captured ChildProcess close handle; Effect ChildProcess does not expose this fork/message protocol.
import * as NodeChildProcess from "node:child_process";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Owned native bootstrap fixtures deliberately inspect link counts and stage physical crash states synchronously.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Native fixture setup/teardown shares synchronous host paths with its retained file/process handles and keeps the existing fixture context.
import * as NodePath from "node:path";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/sql/SqlClient";
import * as Bootstrap from "./AccountJournalBootstrap.ts";
import { NativeJournalHost } from "./NativeAccount.ts";
import * as Journal from "./Journal.ts";
import * as NativeJournal from "./NativeJournal.ts";
import { canonical, failure, storageError } from "./Protocol.ts";
import { openSql } from "./fixtures.ts";

const fixture = Effect.gen(function* () {
  const home = yield* Effect.acquireRelease(
    Effect.sync(() =>
      NodeFS.realpathSync(NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "jones-bootstrap-"))),
    ),
    (owned) => Effect.sync(() => NodeFS.rmSync(owned, { recursive: true, force: true })),
  );
  const account = { home, uid: String(NodeOS.userInfo().uid) };
  const host = NativeJournalHost.of({
    account: Effect.succeed(account),
    open: (filename) => openSql(filename).pipe(Effect.mapError(storageError)),
  });
  const run = Bootstrap.makeForHost().pipe(Effect.provideService(NativeJournalHost, host));
  const path = Journal.fixedAccountJournalPath(account);
  return { home, account, host, run, path, anchor: path.replace(/\.sqlite$/, ".enrollment.json") };
});
const child = Effect.fn("BootstrapRace.child")(function* (account: { home: string; uid: string }) {
  return yield* Effect.acquireRelease(
    Effect.sync(() => {
      const process = NodeChildProcess.fork(
        NodePath.join(import.meta.dirname, "bootstrap-race.fixture.ts"),
        [],
        { stdio: ["ignore", "ignore", "pipe", "ipc"], env: {} },
      );
      let release!: () => void;
      let complete!: (binding: Journal.JournalBinding) => void;
      let fail!: (error: Error) => void;
      const ready = new Promise<void>((resolve) => {
        release = resolve;
      });
      const binding = new Promise<Journal.JournalBinding>((resolve, reject) => {
        complete = resolve;
        fail = reject;
      });
      const closed = new Promise<void>((resolve) => process.once("close", () => resolve()));
      process.on("message", (message: { stage?: string; binding?: Journal.JournalBinding }) => {
        if (message.stage === "candidate_durable") release();
        if (message.binding !== undefined) complete(message.binding);
      });
      let stderr = "";
      process.stderr?.on("data", (chunk: Buffer) => {
        stderr = (stderr + chunk.toString()).slice(-8192);
      });
      process.on("error", fail);
      process.on("close", (code) => {
        if (code !== 0) fail(new Error(`synthetic bootstrap child failed: ${stderr}`));
        release();
      });
      process.send(account);
      return { process, ready, binding, closed };
    }),
    (owned) =>
      Effect.gen(function* () {
        if (owned.process.exitCode === null && owned.process.signalCode === null)
          owned.process.kill("SIGTERM");
        yield* Effect.promise(() => owned.closed);
      }),
  );
});

describe("immutable native account bootstrap", () => {
  it.effect("independent processes choose one anchor, UUID and inode without overwrite", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture;
        const a = yield* child(f.account);
        const b = yield* child(f.account);
        yield* Effect.promise(() => Promise.all([a.ready, b.ready]));
        a.process.send({ continue: true });
        b.process.send({ continue: true });
        const [left, right] = yield* Effect.tryPromise({
          try: () => Promise.all([a.binding, b.binding]),
          catch: storageError,
        });
        assert.deepStrictEqual(left, right);
        yield* Effect.promise(() => Promise.all([a.closed, b.closed]));
        assert.strictEqual(NodeFS.statSync(f.path).nlink, 1);
        const resumed = yield* f.run;
        assert.deepStrictEqual(resumed.binding, left);
      }),
    ),
  );
  it.effect.each([
    "stage_created",
    "stage_durable",
    "candidate_durable",
    "anchor_linked",
    "anchor_durable",
    "journal_linked",
    "journal_durable",
    "stage_unlinked",
  ] as const)("retains unknown files and recovers only the selected stage after %s", (boundary) =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture;
        const result = yield* Effect.exit(
          f.run.pipe(
            Effect.provideService(Bootstrap.BootstrapBoundary, {
              reached: (stage) =>
                stage === boundary ? failure("synthetic_lost_boundary_ack") : Effect.void,
            }),
          ),
        );
        assert.isTrue(Exit.isFailure(result));
        const directory = NodePath.dirname(f.path);
        const before = NodeFS.readdirSync(directory).filter((name) => name.endsWith(".anchor"));
        const anchorBefore = NodeFS.existsSync(f.anchor)
          ? NodeFS.readFileSync(f.anchor, "utf8")
          : null;
        const next = yield* f.run;
        if (anchorBefore !== null) {
          assert.strictEqual(NodeFS.readFileSync(f.anchor, "utf8"), anchorBefore);
          assert.strictEqual(canonical(JSON.parse(anchorBefore).binding), canonical(next.binding));
        }
        for (const retained of before)
          assert.isTrue(NodeFS.existsSync(NodePath.join(directory, retained)));
        assert.strictEqual(NodeFS.statSync(f.path).nlink, 1);
      }),
    ),
  );
  it.effect("a selected anchor with a missing original stage never bootstraps a replacement", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture;
        yield* Effect.exit(
          f.run.pipe(
            Effect.provideService(Bootstrap.BootstrapBoundary, {
              reached: (stage) =>
                stage === "anchor_durable" ? failure("synthetic_crash") : Effect.void,
            }),
          ),
        );
        const anchor = JSON.parse(NodeFS.readFileSync(f.anchor, "utf8"));
        NodeFS.unlinkSync(NodePath.join(NodePath.dirname(f.path), anchor.stagingBasename));
        assert.isTrue(Exit.isFailure(yield* Effect.exit(f.run)));
        assert.isFalse(NodeFS.existsSync(f.path));
      }),
    ),
  );
  it.effect(
    "an unanchored final file, symlink directory and corrupt anchor are held without adoption",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const f = yield* fixture;
          NodeFS.mkdirSync(NodePath.dirname(f.path), { recursive: true, mode: 0o700 });
          NodeFS.writeFileSync(f.path, "unanchored", { mode: 0o600 });
          assert.isTrue(Exit.isFailure(yield* Effect.exit(f.run)));
          assert.strictEqual(NodeFS.readFileSync(f.path, "utf8"), "unanchored");
          const g = yield* fixture;
          NodeFS.symlinkSync(f.home, NodePath.join(g.home, ".local"));
          assert.isTrue(Exit.isFailure(yield* Effect.exit(g.run)));
          const h = yield* fixture;
          NodeFS.mkdirSync(NodePath.dirname(h.path), { recursive: true, mode: 0o700 });
          NodeFS.writeFileSync(h.anchor, "{}", { mode: 0o600 });
          assert.isTrue(Exit.isFailure(yield* Effect.exit(h.run)));
          assert.isFalse(NodeFS.existsSync(h.path));
        }),
      ),
  );
  it.effect(
    "bootstrap refuses a local transaction and pinned make never creates missing state",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const f = yield* fixture;
          const local = yield* openSql(NodePath.join(f.home, "synthetic-local.sqlite"));
          yield* local.withTransaction(
            Effect.gen(function* () {
              assert.isTrue(
                Exit.isFailure(
                  yield* Effect.exit(f.run.pipe(Effect.provideService(SqlClient.SqlClient, local))),
                ),
              );
            }),
          );
          assert.isFalse(NodeFS.existsSync(f.anchor));
          const created = yield* f.run;
          NodeFS.renameSync(f.path, f.path + ".retained");
          assert.isTrue(
            Exit.isFailure(
              yield* Effect.exit(
                NativeJournal.makeForHost(created.binding).pipe(
                  Effect.provideService(NativeJournalHost, f.host),
                ),
              ),
            ),
          );
          assert.isFalse(NodeFS.existsSync(f.path));
        }),
      ),
  );
});
