import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type { Atom } from "effect/reactivity";
import { connectionAtomRuntime } from "../../connection/runtime";
import { appAtomRegistry } from "../../rpc/atomRegistry";

type ConnectionLayer = Atom.Type<typeof connectionAtomRuntime.layer>;

/**
 * Imported by an entry right after install-isolation. Like the guest page, the connection runtime
 * fails to build, so every atom on it keeps its empty value and no environment, session, or saved
 * connection opens. Seeding the app registry also covers handlers that read it directly.
 */
const offline: Effect.Effect<ConnectionLayer> = Effect.die(
  new Error("A design review has no environment connection."),
);
const layerNode = appAtomRegistry.ensureNode(connectionAtomRuntime.layer);
export const connectionHeldOffline = layerNode.currentState() === "uninitialized";
layerNode.setInitialValue(Layer.unwrap(offline));
