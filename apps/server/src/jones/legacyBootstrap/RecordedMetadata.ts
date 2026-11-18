// @effect-diagnostics-next-line nodeBuiltinImport:off - Synchronous canonical hashes bind persisted private identities, matching LegacyBootstrap and CheckoutProtocol.
import * as NodeCrypto from "node:crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import {
  type RecordedAppThread,
  type RecordedRun,
  type RecordedStoredLifecycleEvent,
  RecordedLifecycleEvent,
} from "../../orchestration-v2/RecordedTypes.ts";
import {
  LegacyStorageError,
  type ThreadMetadata,
  type RunMetadata,
  type EventMetadata,
  type ThreadIdentity,
  type RunIdentity,
} from "./StorageSchema.ts";

const decodeLifecycleEvent = Schema.decodeUnknownEffect(RecordedLifecycleEvent);

export const canonical = (value: unknown): string =>
  JSON.stringify(value, (_key, entry: unknown) =>
    entry !== null && typeof entry === "object" && !Array.isArray(entry)
      ? Object.fromEntries(Object.entries(entry).sort(([a], [b]) => a.localeCompare(b)))
      : entry,
  );
export const digest = (value: unknown): string =>
  NodeCrypto.createHash("sha256").update(canonical(value)).digest("hex");
export const threadMetadata = (thread: RecordedAppThread): ThreadMetadata =>
  thread.legacyBootstrapClaim === undefined
    ? {}
    : { legacyBootstrapClaim: thread.legacyBootstrapClaim };
export const runMetadata = (run: RecordedRun): RunMetadata => ({
  ...(run.legacyPreparation === undefined ? {} : { legacyPreparation: run.legacyPreparation }),
  ...(run.legacyReleaseDecision === undefined
    ? {}
    : { legacyReleaseDecision: run.legacyReleaseDecision }),
  ...(run.legacyPreparationFailureDecision === undefined
    ? {}
    : { legacyPreparationFailureDecision: run.legacyPreparationFailureDecision }),
  ...(run.workspaceRunSetupScript === undefined
    ? {}
    : { workspaceRunSetupScript: run.workspaceRunSetupScript }),
  ...(run.legacyBootstrap === undefined ? {} : { legacyBootstrap: run.legacyBootstrap }),
  ...(run.legacyPreparationFailureKnown === undefined
    ? {}
    : { legacyPreparationFailureKnown: run.legacyPreparationFailureKnown }),
});
export const hasMetadata = (metadata: ThreadMetadata | RunMetadata): boolean =>
  Object.keys(metadata).length > 0;
const conflict = () =>
  Effect.fail(new LegacyStorageError({ reason: "metadata_identity_conflict" }));
export const mergeThread = Effect.fn("LegacyMetadata.mergeThread")(function* (
  thread: RecordedAppThread,
  retained: { readonly identity: ThreadIdentity; readonly metadata: ThreadMetadata } | undefined,
) {
  if (retained === undefined) return thread;
  const { identity, metadata } = retained;
  if (
    thread.id !== identity.threadId ||
    thread.projectId !== identity.projectId ||
    DateTime.formatIso(thread.createdAt) !== identity.createdAt ||
    (thread.legacyBootstrapClaim !== undefined &&
      canonical(thread.legacyBootstrapClaim) !== canonical(metadata.legacyBootstrapClaim))
  )
    return yield* conflict();
  return { ...thread, ...metadata };
});
export const mergeRun = Effect.fn("LegacyMetadata.mergeRun")(function* (
  run: RecordedRun,
  retained: { readonly identity: RunIdentity; readonly metadata: RunMetadata } | undefined,
) {
  if (retained === undefined) return run;
  const { identity, metadata } = retained;
  if (
    run.id !== identity.runId ||
    run.threadId !== identity.threadId ||
    run.userMessageId !== identity.userMessageId ||
    DateTime.formatIso(run.requestedAt) !== identity.requestedAt ||
    (run.legacyBootstrap !== undefined &&
      canonical(run.legacyBootstrap) !== canonical(metadata.legacyBootstrap)) ||
    (run.workspaceRunSetupScript !== undefined &&
      run.workspaceRunSetupScript !== metadata.workspaceRunSetupScript) ||
    (run.legacyPreparation !== undefined &&
      run.legacyPreparation.generation !== metadata.legacyPreparation?.generation)
  )
    return yield* conflict();
  // A validated sidecar revision is causally newer than mutable historical inline state.
  const {
    legacyPreparation: _p,
    legacyReleaseDecision: _r,
    legacyPreparationFailureDecision: _f,
    workspaceRunSetupScript: _s,
    legacyBootstrap: _b,
    legacyPreparationFailureKnown: _k,
    ...publicRun
  } = run;
  return { ...publicRun, ...metadata };
});
export const eventMetadata = (stored: RecordedStoredLifecycleEvent): EventMetadata | undefined => {
  const event = stored.event;
  if (
    event.type === "thread.created" ||
    event.type === "thread.metadata-updated" ||
    event.type === "thread.archived" ||
    event.type === "thread.unarchived" ||
    event.type === "thread.deleted"
  ) {
    const metadata = threadMetadata(event.payload);
    return hasMetadata(metadata) ? { kind: "thread", metadata } : undefined;
  }
  if (event.type === "run.created" || event.type === "run.updated") {
    const metadata = runMetadata(event.payload);
    return hasMetadata(metadata) ? { kind: "run", metadata } : undefined;
  }
  return undefined;
};
export const mergeEvent = Effect.fn("LegacyMetadata.mergeEvent")(function* (
  stored: RecordedStoredLifecycleEvent,
  metadata: EventMetadata | undefined,
) {
  if (metadata === undefined) return stored;
  const inline = eventMetadata(stored);
  if (inline !== undefined && canonical(inline) !== canonical(metadata)) return yield* conflict();
  const event = stored.event;
  const threadEvent =
    event.type === "thread.created" ||
    event.type === "thread.metadata-updated" ||
    event.type === "thread.archived" ||
    event.type === "thread.unarchived" ||
    event.type === "thread.deleted";
  if (
    (metadata.kind === "thread" && !threadEvent) ||
    (metadata.kind === "run" && event.type !== "run.created" && event.type !== "run.updated")
  )
    return yield* conflict();
  const merged = yield* decodeLifecycleEvent({
    ...event,
    payload: { ...event.payload, ...metadata.metadata },
  });
  return { ...stored, event: merged };
});
