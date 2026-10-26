// @effect-diagnostics nodeBuiltinImport:off -- Reopen proofs use synthetic SQLite files in a scoped temporary directory.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as Callers from "../runtime/recordedCaller.ts";
import * as Recovery from "../runtime/recovery.ts";
import * as Source from "../providers/localAttestation.ts";
import { AuthOrchestrationReadScope, AuthOrchestrationOperateScope } from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";
import { WorkstreamOperationReadResponse } from "@t3tools/contracts/jones/workstreamOperations";
import migration from "../../../persistence/Migrations/125_JonesWorkstreamsNative.ts";
import { canonicalJson } from "../../operations/encoding.ts";
import { Ports } from "../../operations/ports.ts";
import * as Operations from "../../operations/service.ts";
import {
  fakeControlPlane,
  request,
  createRequest,
  childId,
  hash,
  offline,
} from "../../operations/testFixtures.ts";
import * as BindingStore from "../store/bindingStore.ts";
import * as OperationStore from "../store/operationStore.ts";
import { testLayer, secrets } from "../store/sql.testkit.ts";
import * as DurableEntries from "./durableEntries.ts";
import * as HomeExecutor from "./homeExecutor.ts";

const isReadResponse = Schema.is(WorkstreamOperationReadResponse);

type CP = ReturnType<typeof fakeControlPlane>;
const destination = {
  registry_instance_id: "registry-test",
  owner_id: "owner-synthetic",
  home_environment_id: "home-synthetic",
  home_generation: 5,
  principal_id: "principal-synthetic",
  binding_version: 1,
};
const initialize = Effect.gen(function* () {
  yield* migration;
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO jones_workstreams_keys(key_id,registry_instance_id,scope,state,created_at) VALUES ('test-local','registry-test','local','active','1970-01-01T00:00:00.000Z')`;
  const bindings = yield* BindingStore.BindingStore;
  yield* bindings.bind({ ...destination, state: "active" }, null);
});
const persistence = (filename: string, keys = secrets(), cp?: CP) =>
  Layer.mergeAll(
    DurableEntries.layer,
    HomeExecutor.layer.pipe(
      Layer.provide(
        Layer.succeed(HomeExecutor.HomeTransport, {
          lookup: (target, id) =>
            cp
              ? cp.ports.gateway.readReceipt(id).pipe(
                  Effect.map((result) => {
                    expect(target).toEqual(destination);
                    return result.kind === "receipt"
                      ? {
                          kind: "present" as const,
                          source: "native" as const,
                          receipt: result.receipt,
                        }
                      : {
                          kind: "proven_absent" as const,
                          witness: {
                            server_generation: cp.binding().serverGeneration,
                            registry_version: result.witness_version,
                            archive: "complete" as const,
                          },
                        };
                  }),
                )
              : Effect.fail(offline()),
          resubmit: (target, id, body) => {
            expect(target).toEqual(destination);
            return cp ? cp.ports.gateway.resubmit(id, body) : Effect.fail(offline());
          },
        }),
      ),
    ),
  ).pipe(Layer.provideMerge(testLayer({ filename, keys })), Layer.provideMerge(NodeCrypto.layer));
const serviceLayer = (
  cp: CP,
  filename: string,
  callers?: Callers.RecordedCallers["Service"],
  saved?: (row: OperationStore.Operation) => Effect.Effect<void>,
) =>
  Operations.layer.pipe(
    Layer.provide(
      Layer.effect(
        Ports,
        Effect.gen(function* () {
          const entries = yield* DurableEntries.DurableEntries;
          return Ports.of({
            ...cp.ports,
            native: {
              mode: "native",
              ...(callers ? { callers } : {}),
              entries: saved
                ? { ...entries, save: (...args) => entries.save(...args).pipe(Effect.tap(saved)) }
                : entries,
              executor: yield* HomeExecutor.HomeExecutor,
            },
          });
        }),
      ),
    ),
    Layer.provideMerge(persistence(filename, secrets(), cp)),
  );
const withFile = <A, E, R>(run: (filename: string) => Effect.Effect<A, E, R>) =>
  Effect.scoped(
    Effect.gen(function* () {
      const parent = NodePath.resolve("node_modules/.cache/nw-recorded-recovery/files");
      yield* Effect.promise(() => NodeFSP.mkdir(parent, { recursive: true }));
      const dir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(parent, "operations-")),
      );
      // Retained synthetic DB/WAL evidence; parent root owns cleanup after delivery.
      return yield* run(NodePath.join(dir, "operations.sqlite"));
    }),
  );

it.effect(
  "records encrypted intent before effects and replays the terminal result across restart beyond 30 days",
  () =>
    withFile((filename) =>
      Effect.gen(function* () {
        yield* initialize.pipe(Effect.provide(persistence(filename)));
        const cp = fakeControlPlane();
        const queued = {
          ...request,
          origin: { kind: "queue" as const, request_id: "queue-retained-sentinel" },
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const service = yield* Operations.WorkstreamOperations;
            const store = yield* OperationStore.OperationStore;
            cp.state.beforeSend = (command, body) =>
              Effect.gen(function* () {
                const saved = yield* store.read(queued.operation_id);
                expect(saved?.state).toMatch(/dispatching|in_progress/);
                expect(saved?.request_body).toBe(canonicalJson(queued));
                expect(saved?.request_sha256).toBe(hash(canonicalJson(queued)));
                expect(saved?.destination).toEqual(destination);
                expect(saved?.child_command_ids).toContain(command.command_id);
                expect(JSON.parse(saved!.result_body!).unknown.body).toBe(body);
              }).pipe(Effect.orDie);
            const result = yield* service.submit({ request: queued, wait_ms: 3000 });
            expect(result.state).toBe("committed");
            const sql = yield* SqlClient.SqlClient;
            const [row] = yield* sql<{
              request_envelope: string;
              result_envelope: string;
            }>`SELECT request_envelope,result_envelope FROM jones_workstreams_operations`;
            expect(row!.request_envelope).not.toContain("queue-retained-sentinel");
            expect(row!.result_envelope).not.toContain('"workstream_id":"target"');
            return result;
          }).pipe(Effect.provide(serviceLayer(cp, filename))),
        );
        const writes = cp.state.writes.length;
        cp.state.beforeSend = undefined;
        yield* TestClock.adjust(40 * 86_400_000);
        yield* Effect.scoped(
          Effect.gen(function* () {
            const service = yield* Operations.WorkstreamOperations;
            expect(yield* service.read({ operation_id: queued.operation_id })).toEqual({
              operation: result,
            });
            expect(yield* service.submit({ request: queued, wait_ms: 0 })).toEqual(result);
            expect(cp.state.writes).toHaveLength(writes);
            const store = yield* OperationStore.OperationStore;
            expect((yield* store.read(queued.operation_id))?.child_command_ids).toEqual(
              ["reg", "ver", "mem", "cre"].flatMap((kind) =>
                Array.from({ length: 9 }, (_, i) => childId(queued, kind, i + 1)),
              ),
            );
            expect(yield* store.open()).toEqual([]);
          }).pipe(Effect.provide(serviceLayer(cp, filename))),
        );
      }),
    ),
);

it.effect(
  "retains an unresolved exact child and the thread lock across restart past 24 hours without redispatch",
  () =>
    withFile((filename) =>
      Effect.gen(function* () {
        yield* initialize.pipe(Effect.provide(persistence(filename)));
        const cp = fakeControlPlane();
        const sent = yield* Deferred.make<void>();
        cp.state.beforeSend = () =>
          Deferred.succeed(sent, undefined).pipe(Effect.andThen(Effect.never));
        yield* Effect.scoped(
          Effect.gen(function* () {
            const service = yield* Operations.WorkstreamOperations;
            yield* service.submit({ request, wait_ms: 0 });
            yield* Deferred.await(sent);
          }).pipe(Effect.provide(serviceLayer(cp, filename))),
        );
        const original = yield* Effect.gen(function* () {
          const store = yield* OperationStore.OperationStore;
          return yield* store.read(request.operation_id);
        }).pipe(Effect.provide(persistence(filename)));
        expect(original?.state).toBe("dispatching");
        expect(JSON.parse(original!.result_body!).unknown.command.command_id).toBe(
          childId(request, "reg"),
        );
        const writes = cp.state.writes.length;
        cp.state.beforeSend = undefined;
        yield* TestClock.adjust(25 * 3_600_000);
        yield* Effect.scoped(
          Effect.gen(function* () {
            const service = yield* Operations.WorkstreamOperations;
            yield* service.submit({ request, wait_ms: 0 });
            const response = yield* service.read({ open: true });
            expect(isReadResponse(response)).toBe(true);
            expect("operations" in response && response.operations).toHaveLength(1);
            const conflict = yield* Effect.exit(
              service.submit({
                request: { ...request, operation_id: "another-operation-001" },
                wait_ms: 0,
              }),
            );
            expect(Exit.isFailure(conflict)).toBe(true);
            expect(cp.state.writes).toHaveLength(writes);
            const store = yield* OperationStore.OperationStore;
            expect((yield* store.open())[0]?.destination).toEqual(original?.destination);
            const executor = yield* HomeExecutor.HomeExecutor;
            expect(
              Exit.isFailure(
                yield* Effect.exit(
                  executor.assertSession(original!, { ...cp.binding(), serverGeneration: 6 }),
                ),
              ),
            ).toBe(true);
          }).pipe(Effect.provide(serviceLayer(cp, filename))),
        );
      }),
    ),
);

it.effect("recovers a committed lost reply on restart without executing the child again", () =>
  withFile((filename) =>
    Effect.gen(function* () {
      yield* initialize.pipe(Effect.provide(persistence(filename)));
      const cp = fakeControlPlane();
      const committed = yield* Deferred.make<void>();
      cp.state.afterSend = () =>
        Deferred.succeed(committed, undefined).pipe(Effect.andThen(Effect.never));
      yield* Effect.scoped(
        Effect.gen(function* () {
          const service = yield* Operations.WorkstreamOperations;
          yield* service.submit({ request: createRequest, wait_ms: 0 });
          yield* Deferred.await(committed);
        }).pipe(Effect.provide(serviceLayer(cp, filename))),
      );
      const writes = cp.state.writes.length;
      cp.state.afterSend = undefined;
      yield* Effect.scoped(
        Effect.gen(function* () {
          const service = yield* Operations.WorkstreamOperations;
          const result = yield* service.submit({ request: createRequest, wait_ms: 3000 });
          expect(result.state).toBe("committed");
          expect(cp.state.creates).toBe(1);
          expect(cp.state.writes).toHaveLength(writes);
          expect(yield* service.read({ operation_id: createRequest.operation_id })).toEqual({
            operation: result,
          });
        }).pipe(Effect.provide(serviceLayer(cp, filename))),
      );
    }),
  ),
);

it.effect(
  "absence uses the version witnessed before lookup, even when the next observation advances",
  () =>
    withFile((filename) =>
      Effect.gen(function* () {
        yield* initialize.pipe(Effect.provide(persistence(filename)));
        const cp = fakeControlPlane();
        const waiting = yield* Deferred.make<void>();
        cp.state.beforeSend = () =>
          Effect.sync(() => {
            cp.state.beforeRead = () => {
              cp.state.version++;
              cp.state.beforeRead = undefined;
            };
          }).pipe(Effect.andThen(Effect.fail(offline())));
        cp.state.onSleep = () => Deferred.succeed(waiting, undefined).pipe(Effect.asVoid);
        yield* Effect.scoped(
          Effect.gen(function* () {
            const service = yield* Operations.WorkstreamOperations;
            yield* service.submit({ request: createRequest, wait_ms: 0 });
            yield* Deferred.await(waiting);
            expect(cp.state.writes).toHaveLength(1);
            expect(cp.state.creates).toBe(0);
            const store = yield* OperationStore.OperationStore;
            const row = yield* store.read(createRequest.operation_id);
            expect(row?.state).toBe("reconciling");
            expect(JSON.parse(row!.result_body!).unknown.command.expected_registry_version).toBe(1);
            expect(cp.state.version).toBe(2);
          }).pipe(Effect.provide(serviceLayer(cp, filename))),
        );
      }),
    ),
);

it.effect(
  "a proven-absence witness newer than the old command permits a fenced fresh attempt",
  () =>
    withFile((filename) =>
      Effect.gen(function* () {
        yield* initialize.pipe(Effect.provide(persistence(filename)));
        const cp = fakeControlPlane();
        cp.state.beforeSend = () =>
          Effect.sync(() => {
            cp.state.version++;
            cp.state.beforeSend = undefined;
          }).pipe(Effect.andThen(Effect.fail(offline())));
        const retrying = yield* Deferred.make<void>();
        cp.state.onSleep = (milliseconds) =>
          milliseconds === 250
            ? Deferred.succeed(retrying, undefined).pipe(Effect.asVoid)
            : Effect.void;
        yield* Effect.scoped(
          Effect.gen(function* () {
            const service = yield* Operations.WorkstreamOperations;
            const submitted = yield* service
              .submit({ request: createRequest, wait_ms: 3000 })
              .pipe(Effect.forkChild);
            yield* Deferred.await(retrying);
            yield* TestClock.adjust(250);
            const result = yield* Fiber.join(submitted);
            expect(result.state).toBe("committed");
            expect(cp.state.creates).toBe(1);
            expect(cp.state.writes).toHaveLength(2);
            expect(
              cp.state.writes.map((body) => JSON.parse(body).expected_registry_version),
            ).toEqual([1, 2]);
          }).pipe(Effect.provide(serviceLayer(cp, filename))),
        );
      }),
    ),
);

it.effect("generation-mismatched absence remains unknown and cannot retarget or dispatch", () =>
  withFile((filename) =>
    Effect.gen(function* () {
      yield* initialize.pipe(Effect.provide(persistence(filename)));
      const cp = fakeControlPlane();
      const waiting = yield* Deferred.make<void>();
      cp.state.beforeSend = () =>
        Effect.sync(() => {
          const binding = cp.binding;
          cp.binding = () => ({ ...binding(), serverGeneration: 6 });
          cp.state.version++;
        }).pipe(Effect.andThen(Effect.fail(offline())));
      cp.state.onSleep = () => Deferred.succeed(waiting, undefined).pipe(Effect.asVoid);
      yield* Effect.scoped(
        Effect.gen(function* () {
          const service = yield* Operations.WorkstreamOperations;
          yield* service.submit({ request: createRequest, wait_ms: 0 });
          yield* Deferred.await(waiting);
          const store = yield* OperationStore.OperationStore;
          const row = yield* store.read(createRequest.operation_id);
          expect(row?.state).toBe("reconciling");
          expect(row?.destination.home_generation).toBe(5);
          expect(JSON.parse(row!.result_body!).unknown.command.expected_server_generation).toBe(5);
          expect(cp.state.writes).toHaveLength(1);
          expect(cp.state.creates).toBe(0);
        }).pipe(Effect.provide(serviceLayer(cp, filename))),
      );
    }),
  ),
);

it.effect("missing local keys fail closed and never dispatch or create replacement keys", () =>
  withFile((filename) =>
    Effect.gen(function* () {
      yield* initialize.pipe(Effect.provide(persistence(filename)));
      const keys = secrets();
      keys.delete("workstreams-content-key-test-local");
      yield* Effect.gen(function* () {
        const entries = yield* DurableEntries.DurableEntries;
        expect(
          Exit.isFailure(
            yield* Effect.exit(
              entries.accept(request, [childId(request, "reg")], "1970-01-01T00:00:00.000Z"),
            ),
          ),
        ).toBe(true);
        const store = yield* OperationStore.OperationStore;
        expect(yield* store.read(request.operation_id)).toBeNull();
      }).pipe(Effect.provide(persistence(filename, keys)));
    }),
  ),
);

const callerFixture = (origin = request.origin) => {
  const record: Callers.RecordedCallerV1 = {
    version: 1,
    origin,
    auth: {
      kind: "bearer",
      session_id: "auth-session-private-sentinel",
      subject: "browser",
      method: "bearer-access-token",
    },
    source_environment_id: request.thread.environment_id,
    registry_principal_id: destination.principal_id,
    singleton_binding_version: 1,
    grant_version: 3,
    captured_at: 0,
    source: {
      environment_id: request.thread.environment_id,
      authority_namespace: "authority-private-sentinel",
      store_generation: 1,
      binding_version: 1,
      enrollment_id: "native-enrollment",
      enrollment_session_id: "separate-provider-session",
    },
    destination,
    ceiling: {
      permissions: ["workstreams:read", "workstreams:write"],
      scopes: [AuthOrchestrationReadScope, AuthOrchestrationOperateScope],
      capabilities: [],
      limits: null,
    },
  };
  const state: { value: Callers.CurrentCallerState; capture: Callers.RecordedCallerV1 } = {
    capture: record,
    value: {
      admission: record,
      registry_active: true,
      singleton_active: true,
      source_binding_active: true,
      grant: { active: true, version: 3, permissions: ["workstreams:read", "workstreams:write"] },
      enrollment: {
        enrollment_id: "native-enrollment",
        session_id: "separate-provider-session",
        active: true,
        issued_at: 0,
        expires_at: 9_000_000_000,
      },
      session: {
        session_id: "auth-session-private-sentinel",
        subject: "browser",
        method: "bearer-access-token",
        scopes: record.ceiling.scopes,
        issued_at: 0,
        expires_at: 9_000_000_000,
        revoked: false,
      },
      credential: null,
      run: null,
      trusted_cli_authority_id: null,
    },
  };
  const checkedCeilings: Callers.RecordedCallerV1["ceiling"][] = [];
  return {
    record,
    state,
    checkedCeilings,
    layer: Callers.layer.pipe(
      Layer.provide(
        Layer.succeed(Callers.CallerAuthority, {
          capture: () => Effect.succeed(state.capture),
          refresh: () => Effect.succeed(state.value),
          checkTarget: (_record, ceiling) =>
            Effect.sync(() => {
              checkedCeilings.push(ceiling);
            }),
        }),
      ),
    ),
  };
};
const emptyRaw = Layer.succeed(Recovery.RawRecovery, {
  pending: () => Effect.succeed([]),
  recoverOne: () => Effect.die("unexpected raw recovery"),
});
const childIds = (value: typeof createRequest | typeof request) =>
  ["reg", "ver", "mem", "cre"].flatMap((kind) =>
    Array.from({ length: 9 }, (_, i) => childId(value, kind, i + 1)),
  );
const workerLayer = (
  cp: CP,
  filename: string,
  callers: Callers.RecordedCallers["Service"],
  saved: (row: OperationStore.Operation) => Effect.Effect<void>,
) =>
  Recovery.layer.pipe(
    Layer.provideMerge(serviceLayer(cp, filename, callers, saved)),
    Layer.provide(Layer.succeed(Callers.RecordedCallers, callers)),
    Layer.provide(emptyRaw),
  );

it.effect(
  "initial sweep autonomously resumes a recorded accepted request after restart without submit or read",
  () =>
    withFile((filename) => {
      const f = callerFixture(createRequest.origin);
      return Effect.gen(function* () {
        const callers = yield* Callers.RecordedCallers;
        yield* initialize.pipe(Effect.provide(persistence(filename)));
        yield* Effect.gen(function* () {
          const entries = yield* DurableEntries.DurableEntries;
          const accepted = yield* entries.accept(
            createRequest,
            childIds(createRequest),
            "1970-01-01T00:00:00.000Z",
          );
          expect(accepted.checkpoint).toBeNull();
          expect(accepted.operation.recorded_caller).toEqual(f.record);
        }).pipe(Effect.provide(persistence(filename)));
        const cp = fakeControlPlane();
        const terminal = yield* Deferred.make<void>();
        yield* Effect.scoped(
          Effect.gen(function* () {
            yield* Recovery.NativeWorkstreamsRecovery;
            yield* Deferred.await(terminal);
            expect(cp.state.creates).toBe(1);
            const row = yield* (yield* OperationStore.OperationStore).read(
              createRequest.operation_id,
            );
            expect(row?.state).toBe("committed");
            expect(row?.request_body).toBe(canonicalJson(createRequest));
            const sql = yield* SqlClient.SqlClient;
            yield* sql.unsafe("PRAGMA wal_checkpoint(PASSIVE)");
            for (const suffix of ["", "-wal"]) {
              const bytes = yield* Effect.promise(() =>
                NodeFSP.readFile(filename + suffix).catch(() => Buffer.alloc(0)),
              );
              for (const sentinel of [
                "auth-session-private-sentinel",
                "authority-private-sentinel",
                "separate-provider-session",
              ])
                expect(bytes.includes(Buffer.from(sentinel))).toBe(false);
            }
          }).pipe(
            Effect.provide(
              workerLayer(cp, filename, callers, (row) =>
                row.state === "committed"
                  ? Deferred.succeed(terminal, undefined).pipe(Effect.asVoid)
                  : Effect.void,
              ),
            ),
          ),
        );
      }).pipe(Effect.provide(f.layer));
    }),
);
it.effect(
  "autonomous lookup adopts a lost reply after the effect and before checkpoint without duplicate dispatch",
  () =>
    withFile((filename) => {
      const f = callerFixture(createRequest.origin);
      return Effect.gen(function* () {
        const callers = yield* Callers.RecordedCallers;
        yield* initialize.pipe(Effect.provide(persistence(filename)));
        const cp = fakeControlPlane(),
          effected = yield* Deferred.make<void>();
        cp.state.afterSend = () =>
          Deferred.succeed(effected, undefined).pipe(Effect.andThen(Effect.never));
        yield* Effect.scoped(
          Effect.gen(function* () {
            const operations = yield* Operations.WorkstreamOperations;
            yield* operations.submit({ request: createRequest, wait_ms: 0 });
            yield* Deferred.await(effected);
          }).pipe(Effect.provide(serviceLayer(cp, filename, callers))),
        );
        const bytes = cp.state.writes[0];
        const before = cp.state.writes.length;
        cp.state.afterSend = undefined;
        const terminal = yield* Deferred.make<void>();
        yield* Effect.scoped(
          Effect.gen(function* () {
            yield* Recovery.NativeWorkstreamsRecovery;
            yield* TestClock.adjust(30_000);
            yield* Deferred.await(terminal);
            expect(cp.state.writes).toHaveLength(before);
            expect(cp.state.writes[0]).toBe(bytes);
            expect(cp.state.creates).toBe(1);
          }).pipe(
            Effect.provide(
              workerLayer(cp, filename, callers, (row) =>
                row.state === "committed"
                  ? Deferred.succeed(terminal, undefined).pipe(Effect.asVoid)
                  : Effect.void,
              ),
            ),
          ),
        );
      }).pipe(Effect.provide(f.layer));
    }),
);
it.effect(
  "autonomous proven-absence recovery reuses the exact child bytes and live-submit races share one executor",
  () =>
    withFile((filename) => {
      const f = callerFixture(createRequest.origin);
      return Effect.gen(function* () {
        const callers = yield* Callers.RecordedCallers;
        yield* initialize.pipe(Effect.provide(persistence(filename)));
        const cp = fakeControlPlane(),
          sent = yield* Deferred.make<void>();
        cp.state.beforeSend = () =>
          Deferred.succeed(sent, undefined).pipe(Effect.andThen(Effect.never));
        yield* Effect.scoped(
          Effect.gen(function* () {
            yield* (yield* Operations.WorkstreamOperations).submit({
              request: createRequest,
              wait_ms: 0,
            });
            yield* Deferred.await(sent);
          }).pipe(Effect.provide(serviceLayer(cp, filename, callers))),
        );
        const original = yield* Effect.flatMap(OperationStore.OperationStore, (store) =>
          store.read(createRequest.operation_id),
        ).pipe(Effect.provide(persistence(filename)));
        const child = JSON.parse(original!.result_body!).unknown;
        cp.state.beforeSend = undefined;
        const terminal = yield* Deferred.make<void>();
        yield* Effect.scoped(
          Effect.gen(function* () {
            const worker = yield* Recovery.NativeWorkstreamsRecovery;
            const service = yield* Operations.WorkstreamOperations;
            yield* Effect.all(
              [
                worker.sweep(),
                service.submit({ request: createRequest, wait_ms: 0 }),
                worker.sweep(),
              ],
              { concurrency: "unbounded" },
            );
            yield* Deferred.await(terminal);
            expect(cp.state.creates).toBe(1);
            expect(cp.state.writes.at(-1)).toBe(child.body);
            expect(JSON.parse(cp.state.writes.at(-1)!).command_id).toBe(child.command.command_id);
          }).pipe(
            Effect.provide(
              workerLayer(cp, filename, callers, (row) =>
                row.state === "committed"
                  ? Deferred.succeed(terminal, undefined).pipe(Effect.asVoid)
                  : Effect.void,
              ),
            ),
          ),
        );
      }).pipe(Effect.provide(f.layer));
    }),
);
it.effect(
  "revoked recovery remains open past 24 hours, invisible to another same-kind caller, and retains the thread lock",
  () =>
    withFile((filename) => {
      const f = callerFixture();
      return Effect.gen(function* () {
        const callers = yield* Callers.RecordedCallers;
        yield* initialize.pipe(Effect.provide(persistence(filename)));
        yield* Effect.flatMap(DurableEntries.DurableEntries, (entries) =>
          entries.accept(request, childIds(request), "1970-01-01T00:00:00.000Z"),
        ).pipe(Effect.provide(persistence(filename)));
        f.state.value = { ...f.state.value, session: { ...f.state.value.session!, revoked: true } };
        yield* TestClock.adjust(25 * 3_600_000);
        const cp = fakeControlPlane();
        yield* Effect.scoped(
          Effect.gen(function* () {
            const worker = yield* Recovery.NativeWorkstreamsRecovery;
            const report = yield* worker.sweep();
            expect(report.fenced).toBe(1);
            const store = yield* OperationStore.OperationStore;
            expect(yield* store.open()).toHaveLength(1);
            expect((yield* store.read(request.operation_id))?.state).toBe("accepted");
            expect(cp.state.writes).toHaveLength(0);
            f.state.value = {
              ...f.state.value,
              session: { ...f.state.value.session!, revoked: false },
            };
            f.state.capture = {
              ...f.record,
              auth: {
                kind: "bearer",
                session_id: "different-session",
                subject: "browser",
                method: "bearer-access-token",
              },
            };
            f.state.value = {
              ...f.state.value,
              admission: f.state.capture,
              session: { ...f.state.value.session!, session_id: "different-session" },
            };
            const service = yield* Operations.WorkstreamOperations;
            expect(yield* service.read({ operation_id: request.operation_id })).toEqual({
              operation: null,
            });
            expect(
              Exit.isFailure(yield* Effect.exit(service.submit({ request, wait_ms: 0 }))),
            ).toBe(true);
            f.state.capture = f.record;
            f.state.value = {
              ...f.state.value,
              admission: f.record,
              session: { ...f.state.value.session!, session_id: "auth-session-private-sentinel" },
            };
            const entries = yield* DurableEntries.DurableEntries;
            expect(
              Exit.isFailure(
                yield* Effect.exit(
                  entries.accept(
                    { ...request, operation_id: "different-operation-001" },
                    childIds(request),
                    "1970-01-02T00:00:00.000Z",
                  ),
                ),
              ),
            ).toBe(true);
            expect(cp.state.writes).toHaveLength(0);
          }).pipe(Effect.provide(workerLayer(cp, filename, callers, () => Effect.void))),
        );
      }).pipe(Effect.provide(f.layer));
    }),
);

it.effect(
  "a failed final caller recheck rolls back the accepted request and never leaves recoverable partial evidence",
  () =>
    withFile((filename) => {
      const f = callerFixture();
      return Effect.gen(function* () {
        const real = yield* Callers.RecordedCallers;
        yield* initialize.pipe(Effect.provide(persistence(filename)));
        let checks = 0;
        const changed = Callers.RecordedCallers.of({
          ...real,
          readmitRecorded: (record) =>
            real.readmitRecorded(record).pipe(
              Effect.map((caller) => ({
                ...caller,
                check: (permission, threadId) =>
                  Effect.gen(function* () {
                    if (++checks === 2)
                      return yield* new Source.ProviderError({ code: "not_granted" });
                    yield* caller.check(permission, threadId);
                  }),
              })),
            ),
        });
        yield* Effect.gen(function* () {
          const entries = yield* DurableEntries.DurableEntries;
          expect(
            Exit.isFailure(
              yield* Effect.exit(
                entries
                  .accept(request, childIds(request), "1970-01-01T00:00:00.000Z")
                  .pipe(Effect.provideService(Callers.RecordedCallers, changed)),
              ),
            ),
          ).toBe(true);
          expect(checks).toBe(2);
          const store = yield* OperationStore.OperationStore;
          expect(yield* store.read(request.operation_id)).toBeNull();
          expect(yield* store.open()).toEqual([]);
        }).pipe(Effect.provide(persistence(filename)));
      }).pipe(Effect.provide(f.layer));
    }),
);

it.effect("a live duplicate carries the original permission ceiling into its queued dispatch", () =>
  withFile((filename) => {
    const f = callerFixture(createRequest.origin);
    const original = { ...f.record, ceiling: { ...f.record.ceiling, capabilities: ["original"] } };
    f.state.capture = original;
    f.state.value = { ...f.state.value, admission: original };
    return Effect.gen(function* () {
      const callers = yield* Callers.RecordedCallers;
      yield* initialize.pipe(Effect.provide(persistence(filename)));
      yield* Effect.gen(function* () {
        const entries = yield* DurableEntries.DurableEntries;
        yield* entries.accept(createRequest, childIds(createRequest), "1970-01-01T00:00:00.000Z");
      }).pipe(Effect.provide(persistence(filename)));
      const current = {
        ...original,
        ceiling: { ...original.ceiling, capabilities: ["original", "later"] },
      };
      f.state.capture = current;
      f.state.value = { ...f.state.value, admission: current };
      const cp = fakeControlPlane();
      let queuedChecks = 0;
      cp.state.beforeSend = () =>
        Effect.gen(function* () {
          const caller = yield* Effect.serviceOption(Callers.RecoveryCaller);
          if (caller._tag === "None") return yield* Effect.die("queued caller scope missing");
          yield* caller.value.check("workstreams:write", null).pipe(Effect.orDie);
          expect(f.checkedCeilings.at(-1)?.capabilities).toEqual(["original"]);
          queuedChecks++;
        });
      yield* Effect.scoped(
        Effect.gen(function* () {
          const service = yield* Operations.WorkstreamOperations;
          expect((yield* service.submit({ request: createRequest, wait_ms: 3000 })).state).toBe(
            "committed",
          );
          expect(queuedChecks).toBe(1);
        }).pipe(Effect.provide(serviceLayer(cp, filename, callers))),
      );
    }).pipe(Effect.provide(f.layer));
  }),
);
