import * as NodeOS from "node:os";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import * as SqlClient from "effect/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { AdmissionError, storageError } from "./Protocol.ts";

/** Synthetic hosts replace this boundary; make always supplies the native host.
 * Neither a synthetic account nor a caller's SqlClient reaches the production factory. */
export class NativeJournalHost extends Context.Service<
  NativeJournalHost,
  {
    readonly account: Effect.Effect<
      { readonly home: string; readonly uid: string },
      AdmissionError
    >;
    readonly open: (
      path: string,
    ) => Effect.Effect<SqlClient.SqlClient, AdmissionError, Scope.Scope>;
  }
>()("t3/jones/cleanup/sharedAdmission/NativeAccount/NativeJournalHost") {}

export const nativeHost = NativeJournalHost.of({
  account: Effect.flatMap(HostProcess.Platform, (platform) =>
    Effect.try({
      try: () => {
        // userInfo uses the OS account database. homedir() can consult HOME.
        const account = NodeOS.userInfo();
        if (
          (platform !== "darwin" && platform !== "linux") ||
          account.uid < 0 ||
          process.getuid?.() !== account.uid ||
          process.geteuid?.() !== account.uid
        )
          throw new AdmissionError({ reason: "native_account_identity_unavailable" });
        return { home: account.homedir, uid: String(account.uid) };
      },
      catch: storageError,
    }),
  ),
  open: Effect.fn("NativeJournal.open")(function* (filename: string) {
    const context = yield* Layer.build(
      NodeSqliteClient.layer({
        filename,
        readerWorker: false,
        writerWorker: false,
        checkpointWorker: false,
      }),
    ).pipe(Effect.mapError(storageError));
    return Context.get(context, SqlClient.SqlClient);
  }),
});
