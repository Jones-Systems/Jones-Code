import { assert, describe, it } from "@effect/vitest";
import * as SecureRandom from "effect/crypto/SecureRandom";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- The physical pin fixtures compare bigint inode identity and mutate exact owned files, links and durability state.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Native fixture setup/teardown shares synchronous host paths with its retained file/process handles and keeps the existing fixture context.
import * as NodePath from "node:path";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as NativeJournal from "./NativeJournal.ts";
import * as Journal from "./Journal.ts";
import * as JournalSchema from "./JournalSchema.ts";
import * as Physical from "./PhysicalScope.ts";
import * as P from "./Protocol.ts";
import { fileIdentity, openSql } from "./fixtures.ts";

const fixture = Effect.fn("NativeJournalFixture.make")(function* () {
  const home = yield* Effect.acquireRelease(
    Effect.sync(() =>
      NodeFS.realpathSync(
        NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "jones-native-journal-")),
      ),
    ),
    (owned) => Effect.sync(() => NodeFS.rmSync(owned, { recursive: true, force: true })),
  );
  const account = { home, uid: String(NodeOS.userInfo().uid) };
  const path = Journal.fixedAccountJournalPath(account);
  NodeFS.mkdirSync(NodePath.dirname(path), { recursive: true });
  const observations = Context.get(yield* Layer.build(Physical.layer), P.ScopeObservation);
  // Seed only synthetic sidecar schema. Its connection closes before adapter acquisition.
  const pin = yield* Effect.scoped(
    Effect.gen(function* () {
      const sql = yield* openSql(path);
      const file = fileIdentity(path);
      const binding: Journal.JournalBinding = {
        journalId: SecureRandom.uuidV4Unsafe(),
        schema: 1,
        uid: account.uid,
        canonicalPath: path,
        file: { device: file.device, inode: file.inode, birth: file.birth },
        parent: yield* observations.capture(NodePath.dirname(path)),
      };
      yield* sql`PRAGMA synchronous=FULL`;
      yield* sql`CREATE TABLE jones_common_admission_schema (
      singleton INTEGER PRIMARY KEY CHECK(singleton=1), version INTEGER NOT NULL, binding_json TEXT NOT NULL)`;
      yield* sql`INSERT INTO jones_common_admission_schema VALUES (1,1,${P.canonical(binding)})`;
      return binding;
    }),
  );
  NodeFS.writeFileSync(
    path.replace(/\.sqlite$/, ".enrollment.json"),
    P.canonical({
      format: 1,
      binding: pin,
      schemaContractDigest: JournalSchema.contractDigest,
      stagingBasename: `.workspace-admission-v1.${SecureRandom.uuidV4Unsafe()}.sqlite`,
    }),
    { flag: "wx", mode: 0o600 },
  );
  let opens = 0;
  const host = NativeJournal.NativeJournalHost.of({
    account: Effect.succeed(account),
    open: Effect.fn("NativeJournalFixture.open")(function* (filename: string) {
      opens++;
      assert.strictEqual(filename, path);
      return yield* openSql(filename).pipe(Effect.mapError(P.storageError));
    }),
  });
  return { home, path, pin, host, opens: () => opens };
});

describe("fixed native account journal adapter", () => {
  it.effect(
    "different application bases use independent retained connections to the same physical journal",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const f = yield* fixture();
          const graphs: Journal.JournalConnection["Service"][] = [];
          for (const applicationBase of ["dev-base", "userdata-base"]) {
            NodeFS.mkdirSync(NodePath.join(f.home, applicationBase));
            // Application coordinates are deliberately never passed to the adapter.
            const adapter = yield* NativeJournal.makeForHost(f.pin).pipe(
              Effect.provideService(NativeJournal.NativeJournalHost, f.host),
            );
            const connection = yield* adapter.connectFixedAccountJournal;
            assert.strictEqual(yield* adapter.connectFixedAccountJournal, connection);
            graphs.push(connection);
          }
          const [a, b] = graphs;
          assert.isDefined(a);
          assert.isDefined(b);
          if (a === undefined || b === undefined)
            return yield* Effect.die("missing synthetic graph");
          assert.notStrictEqual(a.sql, b.sql);
          assert.deepStrictEqual(a.binding, b.binding);
          assert.strictEqual(f.opens(), 2);
          const budget = yield* b.sql<{ timeout: number }>`PRAGMA busy_timeout`;
          assert.strictEqual(budget[0]?.timeout, 250);
          yield* b.sql`PRAGMA busy_timeout=0`;
          yield* a.sql.withTransaction(
            Effect.gen(function* () {
              yield* a.sql`UPDATE jones_common_admission_schema SET version=version WHERE singleton=1`;
              assert.isTrue(
                Exit.isFailure(
                  yield* Effect.exit(
                    b.sql.withTransaction(
                      b.sql`UPDATE jones_common_admission_schema SET version=version WHERE singleton=1`,
                    ),
                  ),
                ),
              );
              assert.isTrue(Exit.isFailure(yield* Effect.exit(a.revalidate)));
            }),
          );
          yield* b.sql`PRAGMA busy_timeout=250`;
          yield* b.sql.withTransaction(
            b.sql`UPDATE jones_common_admission_schema SET version=version WHERE singleton=1`,
          );
          // A forbidden nested check permanently invalidates that adapter incarnation.
          assert.isTrue(Exit.isFailure(yield* Effect.exit(a.revalidate)));
          yield* b.revalidate;
        }),
      ),
  );

  it.effect("HOME and XDG spoofing cannot redirect the actual production account resolver", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture();
        const before = NodeFS.readFileSync(f.path);
        const original = {
          HOME: process.env.HOME,
          XDG_STATE_HOME: process.env.XDG_STATE_HOME,
          XDG_DATA_HOME: process.env.XDG_DATA_HOME,
        };
        yield* Effect.acquireRelease(
          Effect.sync(() => {
            process.env.HOME = f.home;
            process.env.XDG_STATE_HOME = NodePath.dirname(f.path);
            process.env.XDG_DATA_HOME = f.home;
          }),
          () =>
            Effect.sync(() => {
              for (const key of ["HOME", "XDG_STATE_HOME", "XDG_DATA_HOME"] as const) {
                if (original[key] === undefined) delete process.env[key];
                else process.env[key] = original[key];
              }
            }),
        );
        // Only a synthetic pin is supplied: the fixed native home must refuse it
        // before any native-home filesystem or SQLite journal access.
        assert.isTrue(Exit.isFailure(yield* Effect.exit(NativeJournal.make(f.pin))));
        assert.deepStrictEqual(NodeFS.readFileSync(f.path), before);
        const adapter = yield* NativeJournal.makeForHost(f.pin).pipe(
          Effect.provideService(NativeJournal.NativeJournalHost, f.host),
        );
        assert.strictEqual(
          (yield* adapter.connectFixedAccountJournal).binding.canonicalPath,
          f.path,
        );
      }),
    ),
  );

  it.effect.each(["missing", "replaced", "alias"] as const)(
    "%s pinned journal refuses before open and never initializes a replacement",
    (fault) =>
      Effect.scoped(
        Effect.gen(function* () {
          const f = yield* fixture();
          const old = `${f.path}.retained`;
          NodeFS.renameSync(f.path, old);
          if (fault === "replaced") NodeFS.writeFileSync(f.path, "synthetic replacement");
          if (fault === "alias") NodeFS.symlinkSync(old, f.path);
          assert.isTrue(
            Exit.isFailure(
              yield* Effect.exit(
                NativeJournal.makeForHost(f.pin).pipe(
                  Effect.provideService(NativeJournal.NativeJournalHost, f.host),
                ),
              ),
            ),
          );
          assert.strictEqual(f.opens(), 0);
          if (fault === "missing") assert.isFalse(NodeFS.existsSync(f.path));
          if (fault === "replaced")
            assert.strictEqual(NodeFS.readFileSync(f.path, "utf8"), "synthetic replacement");
          if (fault === "alias") assert.isTrue(NodeFS.lstatSync(f.path).isSymbolicLink());
        }),
      ),
  );

  it.effect.each(["uid", "path", "parent", "file", "uuid"] as const)(
    "a mismatched local %s pin refuses without rewriting enrollment",
    (fault) =>
      Effect.scoped(
        Effect.gen(function* () {
          const f = yield* fixture();
          const pin = {
            ...f.pin,
            ...(fault === "uid" ? { uid: "not-this-account" } : {}),
            ...(fault === "path" ? { canonicalPath: `${f.path}.other` } : {}),
            ...(fault === "parent" ? { parent: { ...f.pin.parent, canonicalPath: f.home } } : {}),
            ...(fault === "file" ? { file: { ...f.pin.file, inode: "0" } } : {}),
            ...(fault === "uuid" ? { journalId: SecureRandom.uuidV4Unsafe() } : {}),
          };
          if (fault === "uuid") {
            // Synthetic corruption with a mutually matching anchor/pin must still
            // reach and fail the independently retained SQLite binding check.
            const anchorPath = f.path.replace(/\.sqlite$/, ".enrollment.json");
            const anchor = JSON.parse(NodeFS.readFileSync(anchorPath, "utf8"));
            NodeFS.writeFileSync(anchorPath, P.canonical({ ...anchor, binding: pin }));
          }
          const before = NodeFS.readFileSync(f.path);
          assert.isTrue(
            Exit.isFailure(
              yield* Effect.exit(
                NativeJournal.makeForHost(pin).pipe(
                  Effect.provideService(NativeJournal.NativeJournalHost, f.host),
                ),
              ),
            ),
          );
          assert.strictEqual(f.opens(), fault === "uuid" ? 1 : 0);
          assert.deepStrictEqual(NodeFS.readFileSync(f.path), before);
        }),
      ),
  );

  it.effect.each(["file", "parent"] as const)(
    "a changed live %s binding permanently refuses even after its pathname is restored",
    (target) =>
      Effect.scoped(
        Effect.gen(function* () {
          const f = yield* fixture();
          const adapter = yield* NativeJournal.makeForHost(f.pin).pipe(
            Effect.provideService(NativeJournal.NativeJournalHost, f.host),
          );
          const connection = yield* adapter.connectFixedAccountJournal;
          const path = target === "file" ? f.path : NodePath.dirname(f.path);
          yield* Effect.scoped(
            Effect.gen(function* () {
              yield* Effect.acquireRelease(
                Effect.sync(() => {
                  NodeFS.renameSync(path, `${path}.retained`);
                }),
                () => Effect.sync(() => NodeFS.renameSync(`${path}.retained`, path)),
              );
              assert.isTrue(Exit.isFailure(yield* Effect.exit(connection.revalidate)));
              assert.isFalse(NodeFS.existsSync(path));
            }),
          );
          assert.isTrue(NodeFS.existsSync(path));
          assert.isTrue(Exit.isFailure(yield* Effect.exit(adapter.connectFixedAccountJournal)));
          assert.strictEqual(f.opens(), 1);
        }),
      ),
  );

  it.effect(
    "snapshots the local pin, detects durable UUID/durability changes and revokes closed scopes",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const f = yield* fixture();
          let retained: Journal.ProductionJournalAdapter["Service"] | undefined;
          let connection: Journal.JournalConnection["Service"] | undefined;
          yield* Effect.scoped(
            Effect.gen(function* () {
              const callerPin = { ...f.pin, file: { ...f.pin.file } };
              retained = yield* NativeJournal.makeForHost(callerPin).pipe(
                Effect.provideService(NativeJournal.NativeJournalHost, f.host),
              );
              connection = yield* retained.connectFixedAccountJournal;
              callerPin.file.inode = "caller-mutated";
              assert.strictEqual(connection.binding.file.inode, f.pin.file.inode);
              assert.isTrue(Object.isFrozen(connection.binding.file));
              yield* connection.revalidate;
              yield* connection.sql`PRAGMA synchronous = OFF`;
              assert.isTrue(Exit.isFailure(yield* Effect.exit(connection.revalidate)));
            }),
          );
          if (retained === undefined || connection === undefined)
            return yield* Effect.die("missing retained adapter");
          assert.isTrue(Exit.isFailure(yield* Effect.exit(retained.connectFixedAccountJournal)));
          assert.isTrue(Exit.isFailure(yield* Effect.exit(connection.sql`SELECT 1`)));
          const adapter = yield* NativeJournal.makeForHost(f.pin).pipe(
            Effect.provideService(NativeJournal.NativeJournalHost, f.host),
          );
          const live = yield* adapter.connectFixedAccountJournal;
          yield* live.sql`UPDATE jones_common_admission_schema SET binding_json=${P.canonical({ ...f.pin, journalId: SecureRandom.uuidV4Unsafe() })} WHERE singleton=1`;
          assert.isTrue(Exit.isFailure(yield* Effect.exit(live.revalidate)));
        }),
      ),
  );
});
