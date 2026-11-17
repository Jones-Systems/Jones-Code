import * as NodePath from "@effect/platform-node/NodePath";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { ProviderDriverKind, ProviderInstanceId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import { runMigrations } from "../../persistence/Migrations.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as Projections from "../../orchestration-v2/ProjectionStore.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as Replay from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as Registry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import type { ProviderAdapterV2Shape } from "@t3tools/provider-core/server/ProviderAdapter";
import * as Native from "../nativeCreation/NativeCreationRepositorySqlite.ts";
import * as Physical from "../nativeCreation/NativeWorkspacePhysicalPorts.ts";
import * as Workspace from "../nativeCreation/NativeCreationWorkspacePreparation.ts";
import * as Shared from "../cleanup/sharedAdmission/Protocol.ts";
import { fixture, bindingFor } from "../cleanup/sharedAdmission/fixtures.ts";
import * as Receipts from "../../orchestration-v2/CommandReceiptStore.ts";
import * as Repository from "./OrdinaryCheckoutSqlite.ts";
import * as Ordinary from "./OrdinaryCheckout.ts";
import * as Targets from "./CheckoutTarget.ts";
import * as Conflicts from "./CheckoutConflictIntegration.ts";
import * as Intent from "./CheckoutIntent.ts";
import * as Preparation from "./CheckoutPreparation.ts";
import { CheckoutError, digest } from "./CheckoutProtocol.ts";

// Only the physical/preparation custody boundary is synthetic. SQLite, common
// journal, bridge, intent, certificates, sink and orchestrator are real services.
export const makeCommitFixture = Effect.fn("CheckoutCommitFixture.make")(function* (
  options: { readonly lostSharedAck?: boolean } = {},
) {
  const f = yield* fixture();
  const sql = f.b.local;
  yield* runMigrations().pipe(Effect.provideService(SqlClient.SqlClient, sql));
  const database = Layer.succeed(SqlClient.SqlClient, sql);
  let observations = 0;
  let publications = 0;
  let releaseReady = true;
  let lastOperationKey: string | undefined;
  const operations = new Set<string>();
  const results = new Set<string>();
  const outside = Effect.gen(function* () {
    if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
      return yield* new CheckoutError({ reason: "identity" });
    observations++;
  });
  const observation = Layer.succeed(Targets.CheckoutTargetObservation, {
    directory: (path) =>
      outside.pipe(
        Effect.as({
          realPath: path,
          identity: { version: 1 as const, device: "fixture", inode: path, birth: "1" },
        }),
      ),
    git: (path) =>
      outside.pipe(
        Effect.as({
          root: path,
          commonDirectory: `${f.bPath}/.git`,
          gitDirectory: path === f.bPath ? `${path}/.git` : `${f.bPath}/.git/worktrees/work`,
          commonIdentity: { version: 1 as const, device: "fixture", inode: "common", birth: "1" },
          directoryIdentity: { version: 1 as const, device: "fixture", inode: path, birth: "1" },
          branch: path === f.bPath ? "refs/heads/main" : "refs/heads/work",
          registeredWorktrees: [f.bPath, `${f.bPath}/work`],
        }),
      ),
  });
  const files = Layer.succeed(Physical.NativeWorkspaceFileObservation, {
    inspect: (path) =>
      outside.pipe(
        Effect.mapError(
          (cause) =>
            new Workspace.NativeWorkspaceError({ code: "unavailable", message: cause.message }),
        ),
        Effect.as({ realPath: path, birth: "synthetic", kind: "directory" as const }),
      ),
    readFile: () => Effect.succeed(""),
  });
  const repository = Repository.layer.pipe(Layer.provide(Layer.merge(database, NodePath.layer)));
  const targets = Targets.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        observation,
        NodePath.layer,
        Layer.succeed(HostProcess.HomeDirectory, f.directory),
      ),
    ),
  );
  const conflicts = Conflicts.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        database,
        repository,
        observation,
        files,
        NodePath.layer,
        Native.layer.pipe(Layer.provide(database)),
        Receipts.layer.pipe(Layer.provide(database)),
      ),
    ),
  );
  const check = (reservation: Preparation.PreparationReservation) =>
    operations.has(`${reservation.operationId}:${reservation.generation}`)
      ? Effect.void
      : Effect.fail(new CheckoutError({ reason: "identity" }));
  const preparation = Preparation.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        database,
        repository,
        targets,
        conflicts,
        Layer.succeed(Preparation.CheckoutPreparationCustody, {
          checkOutside: (_origin, reservation) => outside.pipe(Effect.andThen(check(reservation))),
          checkResult: (_origin, reservation, result) =>
            outside.pipe(
              Effect.andThen(check(reservation)),
              Effect.andThen(
                results.has(digest(result))
                  ? Effect.void
                  : Effect.fail(new CheckoutError({ reason: "identity" })),
              ),
            ),
          checkReleaseOutside: (_origin, reservation) =>
            outside.pipe(
              Effect.andThen(check(reservation)),
              Effect.andThen(
                Effect.suspend(() =>
                  releaseReady
                    ? Effect.void
                    : Effect.fail(new CheckoutError({ reason: "unavailable" })),
                ),
              ),
            ),
          checkLocal: (_origin, reservation) =>
            Effect.gen(function* () {
              if (Option.isNone(yield* Effect.serviceOption(sql.transactionService)))
                return yield* new CheckoutError({ reason: "identity" });
              yield* check(reservation);
            }),
        }),
      ),
    ),
  );
  const intent = Intent.layer.pipe(
    Layer.provide(Layer.mergeAll(database, repository, targets, conflicts, preparation)),
  );
  const store = Layer.effect(
    EventStore.EventStoreV2,
    Effect.gen(function* () {
      const service = yield* EventStore.EventStoreV2;
      return {
        ...service,
        publishCommitted: (events: Parameters<typeof service.publishCommitted>[0]) =>
          Effect.gen(function* () {
            if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
              return yield* Effect.die("published inside local SQL");
            const accepted =
              lastOperationKey === undefined
                ? null
                : yield* f.b.journal.read(lastOperationKey).pipe(Effect.orDie);
            if (lastOperationKey !== undefined && accepted?.phase !== "accepted")
              return yield* Effect.die("published before common acknowledgement");
            publications += events.length;
            yield* service.publishCommitted(events);
          }),
      };
    }),
  ).pipe(Layer.provide(EventStore.layer));
  const instanceId = ProviderInstanceId.make("codex");
  const adapter = {
    instanceId,
    driver: ProviderDriverKind.make("codex"),
    getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
    planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
    openSession: () => Effect.die("No provider execution in receiving-seam tests"),
  } as ProviderAdapterV2Shape;
  const context = yield* Layer.build(
    Layer.mergeAll(
      database,
      repository,
      targets,
      preparation,
      intent,
      Projections.layer.pipe(Layer.provide(database)),
      Receipts.layer.pipe(Layer.provide(database)),
      Replay.layerWithRegistry({ name: "checkout-commit" }, Registry.layerFromAdapters([adapter]), {
        databaseLayer: database,
        checkoutIntentLayer: intent,
        eventStoreLayer: store,
        runEffectWorker: false,
      }),
    ),
  );
  const reserve = Effect.fn("CheckoutCommitFixture.reserve")(function* (commandId: string) {
    const request = {
      ...bindingFor(f.b.identity.storeId, [f.unrelated], commandId),
      localReference: commandId,
    };
    const journal = options.lostSharedAck
      ? Shared.CommonJournal.of({
          ...f.b.journal,
          commitLocalAcceptance: (record, certificate) =>
            f.b.journal
              .commitLocalAcceptance(record, certificate)
              .pipe(Effect.andThen(Shared.failure("injected_lost_ack_reply"))),
        })
      : f.b.journal;
    const live = yield* Shared.reserveScopes(request).pipe(
      Effect.provide(f.b.context.pipe(Context.add(Shared.CommonJournal, journal))),
    );
    const acceptance: Preparation.LocalAcceptance = {
      live,
      expected: {
        operationKey: Shared.operationKey(request.operation),
        bindingSha256: Shared.digest({ ...request, participant: f.b.identity }),
      },
    };
    lastOperationKey = acceptance.expected.operationKey;
    operations.add(`${commandId}:0`);
    return acceptance;
  });
  return {
    f,
    sql,
    context,
    reserve,
    instanceId,
    sink: Context.get(context, EventSink.EventSinkV2),
    orchestrator: Context.get(context, Orchestrator.OrchestratorV2),
    checkout: Context.get(context, Ordinary.OrdinaryCheckout),
    preparation: Context.get(context, Preparation.CheckoutPreparation),
    targets: Context.get(context, Targets.CheckoutTarget),
    intent: Context.get(context, Intent.CheckoutIntent),
    observe: () => ({ observations, publications }),
    retainResult: (result: Preparation.MaterializedResult) => {
      results.add(digest(result));
    },
    setReleaseReady: (ready: boolean) => {
      releaseReady = ready;
    },
  };
});
