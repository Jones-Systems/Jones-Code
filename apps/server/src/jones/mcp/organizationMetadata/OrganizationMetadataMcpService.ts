import { ProviderRequestKind } from "@t3tools/contracts";
import type { EnvironmentId, ThreadId, OrchestrationV2ThreadShell } from "@t3tools/contracts";
import type {
  NativeInvocationContext,
  OrganizationThreadMetadata,
  OrganizationThreadMetadataPage,
} from "@t3tools/contracts/jones/organizationMetadata";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { HttpServer } from "effect/unstable/http";
import * as NetAddress from "effect/unstable/net/NetAddress";
import packageJson from "../../../../package.json" with { type: "json" };
import { ServerConfig } from "../../../config.ts";
import { ThreadManagementService } from "../../../orchestration-v2/ThreadManagementService.ts";

export class OrganizationMetadataError extends Schema.TaggedError<OrganizationMetadataError>()(
  "OrganizationMetadataError",
  {
    reason: Schema.Literals(["local-operation-failed", "snapshot-sequence-changed"]),
    expectedSnapshotSequence: Schema.optional(Schema.Number),
    snapshotSequence: Schema.optional(Schema.Number),
  },
) {
  override get message(): string {
    return this.reason === "snapshot-sequence-changed"
      ? "Thread metadata snapshot changed; restart pagination."
      : "Unable to read local thread metadata.";
  }
}
export interface MetadataPageInput {
  readonly limit?: number | undefined;
  readonly offset?: number | undefined;
  readonly expectedSnapshotSequence?: number | undefined;
}
export class OrganizationMetadataMcpService extends Context.Service<
  OrganizationMetadataMcpService,
  {
    readonly getInvocationContext: (caller: {
      readonly environmentId: EnvironmentId;
      readonly threadId: ThreadId;
    }) => Effect.Effect<NativeInvocationContext>;
    readonly listThreadMetadata: (
      environmentId: EnvironmentId,
      input: MetadataPageInput,
    ) => Effect.Effect<OrganizationThreadMetadataPage, OrganizationMetadataError>;
  }
>()("t3/jones/mcp/organizationMetadata/OrganizationMetadataMcpService") {}

const iso = (value: DateTime.Utc | null | undefined) =>
  value == null ? null : DateTime.formatIso(value);
const organizationThreadMetadata = (
  thread: OrchestrationV2ThreadShell,
): OrganizationThreadMetadata => ({
  threadId: thread.id,
  title: thread.title.slice(0, 512),
  projectId: thread.projectId,
  pinnedAt: iso(thread.pinnedAt),
  pinOrderKey: thread.pinOrderKey ?? null,
  activeOrderKey: thread.activeOrderKey ?? null,
  snoozedUntil: iso(thread.snoozedUntil),
  settledOverride: thread.settledOverride,
  settledAt: iso(thread.settledAt),
  archivedAt: iso(thread.archivedAt),
  createdAt: DateTime.formatIso(thread.createdAt),
  projectionUpdatedAt: DateTime.formatIso(thread.updatedAt),
  latestUserMessageAt: iso(thread.latestUserMessageAt),
  latestRunId: thread.latestRunId,
  activeRunId: thread.activeRunId,
  status: thread.status,
  latestRunRequestedAt: iso(thread.latestRunRequestedAt),
  latestRunStartedAt: iso(thread.latestRunStartedAt),
  latestRunCompletedAt: iso(thread.latestRunCompletedAt),
  hasPendingApprovals: Schema.is(ProviderRequestKind)(thread.pendingRuntimeRequest?.kind),
  hasPendingUserInput: thread.pendingRuntimeRequest?.kind === "user_input",
  hasActionableProposedPlan: thread.hasActionableProposedPlan,
});

const make = Effect.gen(function* () {
  const snapshots = yield* ThreadManagementService;
  const config = yield* ServerConfig;
  const server = yield* HttpServer.HttpServer;
  return OrganizationMetadataMcpService.of({
    getInvocationContext: (caller) =>
      Effect.gen(function* () {
        const { address } = server;
        let loopbackOrigin: string | null = null;
        if (
          NetAddress.isInetAddress(address) &&
          address.port > 0 &&
          (!NetAddress.isInetAddressV6(address) || address.scopeId === 0) &&
          (NetAddress.isLoopback(address.address) || NetAddress.isUnspecified(address.address))
        ) {
          const host = NetAddress.isUnspecified(address.address)
            ? NetAddress.isIpv4Address(address.address)
              ? NetAddress.ipv4Loopback
              : NetAddress.ipv6Loopback
            : address.address;
          loopbackOrigin = `http://${NetAddress.formatUrlHost(host)}:${address.port}`;
        }
        return {
          environmentId: caller.environmentId,
          threadId: caller.threadId,
          effectiveBaseDir: config.baseDir,
          loopbackOrigin,
          serverVersion: packageJson.version,
          serverGeneration: null,
        };
      }),
    listThreadMetadata: (environmentId, input) =>
      Effect.gen(function* () {
        const snapshot = yield* snapshots
          .getShellSnapshot({ location: "active" })
          .pipe(
            Effect.mapError(
              () => new OrganizationMetadataError({ reason: "local-operation-failed" }),
            ),
          );
        const archived = yield* snapshots
          .getShellSnapshot({ location: "archive" })
          .pipe(
            Effect.mapError(
              () => new OrganizationMetadataError({ reason: "local-operation-failed" }),
            ),
          );
        if (snapshot.snapshotSequence !== archived.snapshotSequence) {
          return yield* new OrganizationMetadataError({
            reason: "snapshot-sequence-changed",
            expectedSnapshotSequence: snapshot.snapshotSequence,
            snapshotSequence: archived.snapshotSequence,
          });
        }
        const threads = [...snapshot.threads, ...archived.archivedThreads]
          .filter((thread) => thread.deletedAt === null)
          .toSorted((a, b) => a.id.localeCompare(b.id));
        if (
          input.expectedSnapshotSequence !== undefined &&
          input.expectedSnapshotSequence !== snapshot.snapshotSequence
        ) {
          return yield* new OrganizationMetadataError({
            reason: "snapshot-sequence-changed",
            expectedSnapshotSequence: input.expectedSnapshotSequence,
            snapshotSequence: snapshot.snapshotSequence,
          });
        }
        const start = input.offset ?? 0;
        const end = start + (input.limit ?? 50);
        const observedAt = DateTime.formatIso(yield* DateTime.now);
        return {
          environmentId,
          snapshotSequence: snapshot.snapshotSequence,
          observedAt,
          threads: threads.slice(start, end).map(organizationThreadMetadata),
          nextOffset: end < threads.length ? end : null,
        };
      }),
  });
});
export const layer = Layer.effect(OrganizationMetadataMcpService, make);
