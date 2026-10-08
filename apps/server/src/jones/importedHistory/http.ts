import {
  AuthOrchestrationOperateScope,
  EnvironmentHttpApi,
  ImportedHistoryUnavailable,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import { requireEnvironmentScope } from "../../auth/http.ts";
import { ThreadManagementService } from "../../orchestration-v2/ThreadManagementService.ts";

export const importedHistoryHttpApiLayer = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "jonesImportedHistory",
  (handlers) =>
    Effect.gen(function* () {
      const threads = yield* ThreadManagementService;
      const unavailable = () =>
        Effect.fail(
          new ImportedHistoryUnavailable({
            reason: "Imported history admission is unavailable or requires reconciliation.",
          }),
        );
      return handlers
        .handle("review", ({ payload }) =>
          Effect.gen(function* () {
            yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
            if (threads.reviewImportedHistory === undefined) return yield* unavailable();
            return yield* threads.reviewImportedHistory(payload).pipe(Effect.catch(unavailable));
          }),
        )
        .handle("start", ({ payload }) =>
          Effect.gen(function* () {
            yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
            if (threads.startWithImportedHistory === undefined) return yield* unavailable();
            return yield* threads.startWithImportedHistory(payload).pipe(Effect.catch(unavailable));
          }),
        )
        .handle("observe", ({ payload }) =>
          Effect.gen(function* () {
            yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
            if (threads.observeImportedHistoryStart === undefined) return yield* unavailable();
            return yield* threads
              .observeImportedHistoryStart(payload)
              .pipe(Effect.catch(unavailable));
          }),
        );
    }),
);
