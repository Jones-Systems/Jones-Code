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
