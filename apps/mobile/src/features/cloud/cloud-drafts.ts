import { EnvironmentRegistry, removedWithRelay } from "@t3tools/client-runtime/connection";
import { createRuntimeCommand } from "@t3tools/client-runtime/state/runtime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { msg } from "gt-react-native";

import { connectionAtomRuntime } from "../../connection/runtime";
import { m } from "../../i18n/messages";
import {
  archiveCloudComposerDrafts,
  beginCloudOutboxArchive,
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

export const removeCloudEnvironments = createRuntimeCommand(connectionAtomRuntime, {
  label: "cloud:preserve-drafts-and-remove-environments",
  execute: Effect.fn("removeCloudEnvironments")(function* (accountId: string | null) {
    const registry = yield* EnvironmentRegistry.EnvironmentRegistry;
    const entries = yield* SubscriptionRef.get(registry.entries);
    const environmentIds = new Set(
      [...entries.values()].filter(removedWithRelay).map((entry) => entry.target.environmentId),
    );
    const archiveError = (cause: unknown) =>
      new CloudDraftArchiveError({
        environmentCount: environmentIds.size,
        hasAccountId: accountId !== null,
        cause,
      });
    yield* Effect.acquireUseRelease(
      Effect.try({ try: () => beginCloudOutboxArchive(environmentIds), catch: archiveError }),
      () =>
        Effect.tryPromise({
          // Resolve draft owners against every environment, including retained direct routes.
          try: () =>
            archiveCloudComposerDrafts(
              accountId,
              environmentIds,
              [...entries.values()].map((entry) => entry.target.environmentId),
            ),
          catch: archiveError,
        }).pipe(Effect.uninterruptible, Effect.andThen(registry.removeRelayEnvironments())),
      (release) => Effect.sync(release),
    );
  }),
});
