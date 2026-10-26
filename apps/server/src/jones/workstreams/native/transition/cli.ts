import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as NativeSql from "../store/sql.ts";
import * as Registry from "../store/registryStore.ts";
import * as Archive from "../store/archiveStore.ts";
import * as Identity from "../crypto/identityIndex.ts";
import * as Keys from "../crypto/keyProvider.ts";
import { plan } from "./plan.ts";
import { apply } from "./apply.ts";
import { readback } from "./readback.ts";
import { activate } from "./activate.ts";
import { prepareReverse, reverseExport } from "./reverseExport.ts";
import {
  prepareTransfer,
  transfer,
  readTransfer,
  applyTransferExtensions,
  readbackTransfer,
  completeTransfer,
} from "./transfer.ts";
type Bound<F extends (...args: never[]) => Effect.Effect<unknown, unknown, unknown>> = (
  ...args: Parameters<F>
) => Effect.Effect<Effect.Success<ReturnType<F>>, Effect.Error<ReturnType<F>>>;
export class WorkstreamsTransition extends Context.Service<
  WorkstreamsTransition,
  {
    readonly plan: Bound<typeof plan>;
    readonly apply: Bound<typeof apply>;
    readonly readback: Bound<typeof readback>;
    readonly activate: Bound<typeof activate>;
    readonly prepareReverse: Bound<typeof prepareReverse>;
    readonly reverseExport: Bound<typeof reverseExport>;
    readonly prepareTransfer: Bound<typeof prepareTransfer>;
    readonly transfer: Bound<typeof transfer>;
    readonly readTransfer: Bound<typeof readTransfer>;
    readonly applyTransferExtensions: Bound<typeof applyTransferExtensions>;
    readonly readbackTransfer: Bound<typeof readbackTransfer>;
    readonly completeTransfer: Bound<typeof completeTransfer>;
  }
>()("t3/jones/workstreams/native/transition/cli/WorkstreamsTransition") {}
const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient,
    db = yield* NativeSql.NativeSql,
    registry = yield* Registry.RegistryStore,
    archive = yield* Archive.ArchiveStore,
    identity = yield* Identity.WorkstreamsIdentityIndex,
    keys = yield* Keys.WorkstreamsKeyProvider;
  const provide = <A, E>(
    effect: Effect.Effect<
      A,
      E,
      | SqlClient.SqlClient
      | NativeSql.NativeSql
      | Registry.RegistryStore
      | Archive.ArchiveStore
      | Identity.WorkstreamsIdentityIndex
      | Keys.WorkstreamsKeyProvider
    >,
  ) =>
    effect.pipe(
      Effect.provideService(SqlClient.SqlClient, sql),
      Effect.provideService(NativeSql.NativeSql, db),
      Effect.provideService(Registry.RegistryStore, registry),
      Effect.provideService(Archive.ArchiveStore, archive),
      Effect.provideService(Identity.WorkstreamsIdentityIndex, identity),
      Effect.provideService(Keys.WorkstreamsKeyProvider, keys),
    );
  return WorkstreamsTransition.of({
    plan: (...args) => provide(plan(...args)),
    apply: (...args) => provide(apply(...args)),
    readback: (...args) => provide(readback(...args)),
    activate: (...args) => provide(activate(...args)),
    prepareReverse: (...args) => provide(prepareReverse(...args)),
    reverseExport: (...args) => provide(reverseExport(...args)),
    prepareTransfer: (...args) => provide(prepareTransfer(...args)),
    transfer: (...args) => provide(transfer(...args)),
    readTransfer: (...args) => provide(readTransfer(...args)),
    applyTransferExtensions: (...args) => provide(applyTransferExtensions(...args)),
    readbackTransfer: (...args) => provide(readbackTransfer(...args)),
    completeTransfer: (...args) => provide(completeTransfer(...args)),
  });
});
/** @public Operator composition supplies J1 persistence and explicitly provisioned keys. */
export const layer = Layer.effect(WorkstreamsTransition, make);
/** Thin CLI adapter: the operator composition owns input decoding, encrypted I/O and approval. */
export const nativeTransitionCommand = Effect.fn("WorkstreamsTransition.cli")(function* (
  command:
    | { readonly action: "plan"; readonly input: Parameters<typeof plan>[0] }
    | {
        readonly action: "apply";
        readonly plan: Parameters<typeof apply>[0];
        readonly limit?: number;
      }
    | { readonly action: "readback"; readonly plan: Parameters<typeof readback>[0] }
    | {
        readonly action: "activate";
        readonly plan: Parameters<typeof activate>[0];
        readonly intent: Parameters<typeof activate>[1];
      },
) {
  const service = yield* WorkstreamsTransition;
  switch (command.action) {
    case "plan":
      return yield* service.plan(command.input);
    case "apply":
      return yield* service.apply(command.plan, command.limit);
    case "readback":
      return yield* service.readback(command.plan);
    case "activate":
      return yield* service.activate(command.plan, command.intent);
  }
});
