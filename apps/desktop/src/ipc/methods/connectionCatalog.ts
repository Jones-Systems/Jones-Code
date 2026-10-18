import { StoredConnectionCompatibilityHint } from "@t3tools/client-runtime/platform";
import { EnvironmentId } from "@t3tools/contracts";
import * as DesktopConnectionCompatibilityHintStore from "../../app/DesktopConnectionCompatibilityHintStore.ts";
import * as DesktopEnvironment from "../../app/DesktopEnvironment.ts";
import * as ElectronSafeStorage from "../../electron/ElectronSafeStorage.ts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import * as DesktopConnectionCatalogStore from "../../app/DesktopConnectionCatalogStore.ts";
import * as IpcChannels from "../channels.ts";
import * as DesktopIpc from "../DesktopIpc.ts";

export const getConnectionCatalog = DesktopIpc.makeIpcMethod({
  channel: IpcChannels.GET_CONNECTION_CATALOG_CHANNEL,
  payload: Schema.Void,
  result: Schema.NullOr(Schema.String),
  handler: Effect.fn("desktop.ipc.connectionCatalog.get")(function* () {
    const store = yield* DesktopConnectionCatalogStore.DesktopConnectionCatalogStore;
    return Option.getOrNull(yield* store.get);
  }),
});

export const setConnectionCatalog = DesktopIpc.makeIpcMethod({
  channel: IpcChannels.SET_CONNECTION_CATALOG_CHANNEL,
  payload: Schema.String,
  result: Schema.Boolean,
  handler: Effect.fn("desktop.ipc.connectionCatalog.set")(function* (catalog) {
    const store = yield* DesktopConnectionCatalogStore.DesktopConnectionCatalogStore;
    return yield* store.set(catalog);
  }),
});

export const clearConnectionCatalog = DesktopIpc.makeIpcMethod({
  channel: IpcChannels.CLEAR_CONNECTION_CATALOG_CHANNEL,
  payload: Schema.Void,
  result: Schema.Void,
  handler: Effect.fn("desktop.ipc.connectionCatalog.clear")(function* () {
    const store = yield* DesktopConnectionCatalogStore.DesktopConnectionCatalogStore;
    yield* store.clear;
  }),
});

export const getConnectionCatalogSnapshot = DesktopIpc.makeIpcMethod({
  channel: IpcChannels.GET_CONNECTION_CATALOG_SNAPSHOT_CHANNEL,
  payload: Schema.Void,
  result: Schema.NullOr(Schema.String),
  handler: Effect.fn("desktop.ipc.connectionCatalog.snapshot")(function* () {
    const store = yield* DesktopConnectionCatalogStore.DesktopConnectionCatalogStore;
    return Option.getOrNull(yield* store.snapshot);
  }),
});

const hintStore = Effect.gen(function* () {
  const environment = yield* DesktopEnvironment.DesktopEnvironment;
  const protection = yield* ElectronSafeStorage.ElectronSafeStorage;
  return yield* DesktopConnectionCompatibilityHintStore.make({
    directory: environment.stateDir,
    protection,
  });
});

export const listConnectionCompatibilityHints = DesktopIpc.makeIpcMethod({
  channel: IpcChannels.LIST_CONNECTION_COMPATIBILITY_HINTS_CHANNEL,
  payload: Schema.Void,
  result: Schema.Array(StoredConnectionCompatibilityHint),
  handler: Effect.fn("desktop.ipc.connectionCompatibilityHints.list")(function* () {
    return yield* (yield* hintStore).read;
  }),
});

export const setConnectionCompatibilityHint = DesktopIpc.makeIpcMethod({
  channel: IpcChannels.SET_CONNECTION_COMPATIBILITY_HINT_CHANNEL,
  payload: Schema.Struct({
    environmentId: EnvironmentId.check(Schema.isMaxLength(256)),
    routeBinding: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(16_384)),
    kind: Schema.NullOr(Schema.Literal("outdated-update")),
  }),
  result: Schema.Boolean,
  handler: Effect.fn("desktop.ipc.connectionCompatibilityHints.set")(function* (input) {
    return yield* (yield* hintStore).set(input.environmentId, input.routeBinding, input.kind);
  }),
});
