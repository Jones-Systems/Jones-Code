// One captured synthetic-account child, synchronized through IPC rather than sleeps.
import * as Effect from "effect/Effect";
import * as Bootstrap from "./AccountJournalBootstrap.ts";
import { NativeJournalHost } from "./NativeAccount.ts";
import { openSql } from "./fixtures.ts";
import { storageError } from "./Protocol.ts";

const input = await new Promise<{ home: string; uid: string }>((resolve) =>
  process.once("message", (message) => resolve(message as { home: string; uid: string })),
);
await Effect.runPromise(
  Effect.scoped(
    Bootstrap.makeForHost().pipe(
      Effect.provideService(NativeJournalHost, {
        account: Effect.succeed(input),
        open: (filename) => openSql(filename).pipe(Effect.mapError(storageError)),
      }),
      Effect.provideService(Bootstrap.BootstrapBoundary, {
        reached: (stage) =>
          stage !== "candidate_durable"
            ? Effect.void
            : Effect.promise(
                () =>
                  new Promise<void>((resolve) => {
                    process.once("message", () => resolve());
                    process.send?.({ stage });
                  }),
              ),
      }),
      Effect.tap((result) =>
        Effect.sync(() => {
          process.send?.({ binding: result.binding });
        }),
      ),
    ),
  ),
).then(
  () => {
    process.disconnect();
  },
  async (cause) => {
    try {
      await Effect.runPromise(Effect.logError(cause));
    } finally {
      process.exitCode = 1;
      process.disconnect();
    }
  },
);
