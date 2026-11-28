import {
  connectionRoutes,
  EnvironmentRegistry,
  isLearned,
  removedWithRelay,
  type ConnectionCatalogEntry,
} from "@t3tools/client-runtime/connection";
import { createRuntimeCommand } from "@t3tools/client-runtime/state/runtime";
import type { EnvironmentId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { msg } from "gt-react-native";

import { connectionAtomRuntime } from "../../connection/runtime";
import { m } from "../../i18n/messages";
import { archiveAndClearCloudOutbox } from "../../state/thread-outbox-removal";
import {
  beginCloudOutboxArchive,
  finishCloudOutboxArchive,
  getRestoredCloudEnvironmentIds,
} from "../../state/use-composer-drafts";

export class CloudDraftArchiveError extends Schema.TaggedError<CloudDraftArchiveError>()(
  "CloudDraftArchiveError",
  {
    environmentCount: Schema.Number,
    hasAccountId: Schema.Boolean,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return m(
      msg(
        "{count, plural, other {Could not preserve local drafts for {count} cloud environments before sign-out.}}",
        { count: this.environmentCount },
      ),
    );
  }
}

function retainsDirectRoute(entry: ConnectionCatalogEntry | undefined): boolean {
  return (
    entry !== undefined &&
    connectionRoutes(entry).some(
      (route) => route.target._tag !== "RelayConnectionTarget" && !isLearned(route),
    )
  );
}

export const preserveCloudDraftsAndRemoveEnvironments = Effect.fn("removeCloudEnvironments")(
  function* (accountId: string | null) {
    const registry = yield* EnvironmentRegistry.EnvironmentRegistry;
    const entries = yield* SubscriptionRef.get(registry.entries);
    const registered = new Set(
      [...entries.values()].filter(removedWithRelay).map((entry) => entry.target.environmentId),
    );
    const restoredOnly = new Set<EnvironmentId>();
    const archiveError = (cause: unknown) =>
      new CloudDraftArchiveError({
        environmentCount: registered.size + restoredOnly.size,
        hasAccountId: accountId !== null,
        cause,
      });
    const fence = (ids: ReadonlySet<EnvironmentId>) =>
      Effect.acquireRelease(
        Effect.try({ try: () => beginCloudOutboxArchive(ids), catch: archiveError }),
        (release) => Effect.sync(release),
      );

    yield* Effect.scoped(
      Effect.gen(function* () {
        yield* fence(registered);
        const restored = yield* Effect.tryPromise({
          try: getRestoredCloudEnvironmentIds,
          catch: archiveError,
        });
        const retained = restored.filter((id) => retainsDirectRoute(entries.get(id)));
        for (const id of restored) {
          if (!registered.has(id) && !retainsDirectRoute(entries.get(id))) restoredOnly.add(id);
        }
        if (restoredOnly.size > 0) yield* fence(restoredOnly);

        const environmentIds = new Set([...registered, ...restoredOnly]);
        yield* Effect.tryPromise({
          // Resolve draft owners against every environment, including retained direct routes.
          try: () =>
            archiveAndClearCloudOutbox(accountId, environmentIds, [
              ...new Set([...entries.keys(), ...environmentIds]),
            ]),
          catch: archiveError,
        }).pipe(Effect.uninterruptible);
        yield* registry.removeRelayEnvironments();
        if (retained.length > 0) {
          const remaining = yield* SubscriptionRef.get(registry.entries);
          // Keep the durable owner if a route we relied on disappeared during cleanup.
          if (retained.some((id) => !retainsDirectRoute(remaining.get(id)))) {
            return yield* archiveError(new Error("A retained direct route was removed."));
          }
        }
        yield* Effect.tryPromise({ try: finishCloudOutboxArchive, catch: archiveError });
      }),
    );
  },
);

export const removeCloudEnvironments = createRuntimeCommand(connectionAtomRuntime, {
  label: "cloud:preserve-drafts-and-remove-environments",
  execute: preserveCloudDraftsAndRemoveEnvironments,
});
