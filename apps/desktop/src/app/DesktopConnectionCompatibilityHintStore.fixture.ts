// The parent owns this synthetic child, its IPC lifetime and scratch directory.
import * as NodeServices from "@effect/platform-node/NodeServices";
import { EnvironmentId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Store from "./DesktopConnectionCompatibilityHintStore.ts";

const directory = process.argv[2];
if (!directory || !process.send)
  throw new Error("Requires an owned fixture directory and IPC parent.");
const store = await Effect.runPromise(
  Store.make({
    directory,
    protection: {
      isEncryptionAvailable: Effect.succeed(true),
      encryptString: (value) => Effect.succeed(new TextEncoder().encode(`fixture:${value}`)),
      decryptString: (value) => Effect.succeed(new TextDecoder().decode(value).slice(8)),
    },
  }).pipe(Effect.provide(NodeServices.layer)),
);
let pending = Promise.resolve();
process.on("error", () => {
  process.exitCode = 1;
});
function send(message: object) {
  if (!process.connected || !process.send) return;
  try {
    process.send(message, (error) => {
      if (error) process.exitCode = 1;
    });
  } catch {
    process.exitCode = 1;
  }
}
process.on(
  "message",
  (message: {
    id: number;
    operation: "read" | "set";
    environmentId?: string;
    routeBinding?: string;
    kind?: "outdated-update" | null;
  }) => {
    pending = pending.then(async () => {
      try {
        const result =
          message.operation === "read"
            ? await Effect.runPromise(store.read)
            : await Effect.runPromise(
                store.set(
                  EnvironmentId.make(message.environmentId!),
                  message.routeBinding!,
                  message.kind ?? null,
                ),
              );
        send({ id: message.id, ok: true, result });
      } catch (error) {
        send({ id: message.id, ok: false, error: String(error) });
      }
    });
  },
);
process.on("disconnect", () => {
  void pending.then(() => process.exit(process.exitCode ?? 0));
});
send({ ready: true });
