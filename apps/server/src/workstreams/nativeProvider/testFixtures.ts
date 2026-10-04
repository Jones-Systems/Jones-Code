import conformance from "../../../../../packages/contracts/contracts/workstreams-t3-provider/v1/fixtures/conformance.json" with { type: "json" };
import {
  WorkstreamsNativeContext,
  WorkstreamsNativeSettlementRequest,
  WorkstreamsNativeAttestationRequest,
  WorkstreamsNativeTerminalResult,
  CommandId,
  EventId,
  ThreadId,
  AuthSessionId,
  EnvironmentAuthenticatedPrincipal,
  type NativeCommandIdentityV2,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { type NativeProviderAttempt, type NativeProviderAttempts } from "./attemptRepository.ts";
import { NATIVE_PROVIDER_SCOPES, type NativeProviderEnrollmentBinding } from "./enrollment.ts";
import {
  makeWorkstreamsNativeProvider,
  nativeProviderSettlementFromFacts,
  sha256Bytes,
  type NativeProviderPorts,
} from "./service.ts";
import type { CommandReceiptV2 } from "../../orchestration-v2/CommandReceiptStore.ts";
import type { NativeCommandFactsV2 } from "../../orchestration-v2/EventSink.ts";
import type { NativeWorkstreamSettlementInputV2 } from "../../orchestration-v2/Orchestrator.ts";
import type { OrchestrationCommandEventMetadata } from "../../persistence/Services/OrchestrationEventStore.ts";

const fixture = (name: string) => conformance.cases.find((entry) => entry.name === name)?.value;
export const enrolledContext = Schema.decodeUnknownSync(WorkstreamsNativeContext)(
  fixture("Context"),
);
export const request = Schema.decodeUnknownSync(WorkstreamsNativeSettlementRequest)(
  fixture("SettlementRequest"),
);
export const attestationRequest = Schema.decodeUnknownSync(WorkstreamsNativeAttestationRequest)(
  fixture("AttestationRequest"),
);
export const binding: NativeProviderEnrollmentBinding = {
  ...enrolledContext,
  session_id: "session-synthetic",
  registry_origin: "https://registry.invalid",
  scopes: Object.values(NATIVE_PROVIDER_SCOPES),
};
export const nativeTestPrincipal: EnvironmentAuthenticatedPrincipal["Service"] = {
  sessionId: AuthSessionId.make(binding.session_id),
  subject: `workstreams-native:${binding.enrollment_id}`,
  method: "bearer-access-token",
  scopes: new Set(Object.values(NATIVE_PROVIDER_SCOPES)),
};
export const nativeTestPrincipalLayer = Layer.succeed(
  EnvironmentAuthenticatedPrincipal,
  nativeTestPrincipal,
);
export const requestText = Schema.encodeSync(
  Schema.fromJsonString(WorkstreamsNativeSettlementRequest),
)(request);
export const requestBytesSha256 = sha256Bytes(requestText);
export const canonicalCommittedResult = Schema.decodeUnknownSync(WorkstreamsNativeTerminalResult)(
  fixture("TerminalResult"),
);
export const now = "2026-01-01T00:00:00.000Z";

export const makeProviderFixture = () => {
  const attempts = new Map<string, NativeProviderAttempt>();
  const receipts = new Map<string, CommandReceiptV2>();
  const identities = new Map<string, NativeCommandIdentityV2>();
  const expectedIdentities = new Map<string, NativeCommandIdentityV2>();
  const incarnation = { eventId: EventId.make("synthetic-v2-birth"), sequence: 1 };
  const events = new Map<string, ReadonlyArray<OrchestrationCommandEventMetadata>>();
  const calls: {
    readonly commandId: string;
    readonly threadId: string;
    readonly action: string;
  }[] = [];
  const key = (value: Pick<typeof request, "owner_id" | "principal_id" | "command_id">) =>
    `${value.owner_id}/${value.principal_id}/${value.command_id}`;
  const repo: NativeProviderAttempts["Service"] = {
    get: (input) => Effect.sync(() => Option.fromUndefinedOr(attempts.get(key(input)))),
    reserve: (attempt) =>
      Effect.sync(() => {
        const existing = attempts.get(key(attempt.request));
        if (existing) return existing;
        const value = { ...attempt };
        attempts.set(key(attempt.request), value);
        return value;
      }),
    startDispatch: (input, at) =>
      Effect.sync(() => {
        const attempt = attempts.get(key(input));
        if (!attempt || attempt.dispatchStartedAt !== null) return false;
        attempts.set(key(input), { ...attempt, dispatchStartedAt: at });
        return true;
      }),
  };
  // Opaque synthetic expectations exercise correlation; they do not qualify a native source or runtime.
  const recordIdentity = (commandId: string, action: "settle" | "unsettle" = "settle") => {
    const identity: NativeCommandIdentityV2 = {
      kind: "workstream_settlement",
      version: 2,
      commandId: CommandId.make(commandId),
      commandType: action === "settle" ? "thread.settle" : "thread.unsettle",
      aggregateKind: "thread",
      aggregateId: ThreadId.make(request.identity.native_id),
      normalizedCommandDigest: "a".repeat(64),
      bindingDigest: "b".repeat(64),
    };
    identities.set(commandId, identity);
    expectedIdentities.set(commandId, identity);
    return identity;
  };
  const commit = (
    commandId: string,
    action: "settle" | "unsettle" = "settle",
    companions = false,
  ) => {
    recordIdentity(commandId, action);
    receipts.set(commandId, {
      commandId: CommandId.make(commandId),
      commandType: action === "settle" ? "thread.settle" : "thread.unsettle",
      threadId: ThreadId.make(request.identity.native_id),
      acceptedAt: DateTime.makeUnsafe(now),
      resultSequence: companions ? 3 : 2,
      status: "accepted",
      error: null,
    });
    events.set(commandId, [
      {
        eventId: EventId.make("event-synthetic"),
        commandId: CommandId.make(commandId),
        aggregateKind: "thread",
        aggregateId: ThreadId.make(request.identity.native_id),
        sequence: 2,
        type: action === "settle" ? "thread.settled" : "thread.unsettled",
        occurredAt: now,
        applicationEventVersion: 2,
      },
      ...(companions
        ? [
            {
              eventId: EventId.make("event-unpinned"),
              commandId: CommandId.make(commandId),
              aggregateKind: "thread" as const,
              aggregateId: ThreadId.make(request.identity.native_id),
              sequence: 3,
              type: "thread.unpinned" as const,
              occurredAt: now,
              applicationEventVersion: 2,
            },
          ]
        : []),
    ]);
  };
  const ports: NativeProviderPorts = {
    authority: {
      readCurrent: Effect.succeed({
        environmentId: binding.source_instance_id,
        authorityNamespace: binding.authority_namespace,
        storeGeneration: binding.store_generation,
      }),
    },
    threadExists: () => Effect.succeed(true),
    attempts: repo,
    eventSink: {
      getThreadIncarnation: () => Effect.succeed(incarnation),
      readNativeCommandFacts: ({ threadId, commandId, authority }) =>
        Effect.sync(
          () =>
            ({
              commandId,
              threadId,
              receipt: receipts.get(commandId) ?? null,
              identity: identities.get(commandId) ?? null,
              workstreamWitness: null,
              eventMetadata: events.get(commandId) ?? [],
              eventMetadataOverflow: (events.get(commandId)?.length ?? 0) > 256,
              events: [],
              snapshotSequence: 3,
              targetEventSequence: 3,
              incarnation,
              creationProvenance: "native_created",
              projection: null,
              creationHistory: [],
              nativeCreationHistory: null,
              commitSnapshot: {
                commandId,
                threadId,
                targetEventSequence: 3,
                incarnation,
                creationProvenance: "native_created",
                records: {},
                authority: authority ?? {},
                authorityRecords: {},
              },
            }) satisfies NativeCommandFactsV2,
        ),
    },
    build: Effect.succeed(Option.some(binding.build)),
    orchestrator: {
      dispatchNativeWorkstreamSettlement: (input) =>
        Effect.sync(() => {
          const command = nativeSettlementCommand(input);
          if (command.type !== "thread.settle" && command.type !== "thread.unsettle")
            throw new Error("Unexpected native command.");
          const attempt = attempts.get(key(input.request));
          if (!attempt || attempt.dispatchStartedAt === null || attempt !== input.attempt)
            throw new Error("Dispatch preceded durable start.");
          calls.push({
            commandId: command.commandId,
            threadId: command.threadId,
            action: command.type,
          });
          commit(command.commandId, command.type === "thread.settle" ? "settle" : "unsettle");
          return { sequence: 2, storedEvents: [] };
        }),
    },
  };
  return {
    attempts,
    receipts,
    identities,
    recordIdentity,
    events,
    calls,
    ports,
    key,
    commit,
    provider: () => makeWorkstreamsNativeProvider(ports),
    observeExpected: (input = request) =>
      Effect.gen(function* () {
        const attempt = attempts.get(key(input));
        if (attempt === undefined) throw new Error("Synthetic attempt is missing.");
        const facts = yield* ports.eventSink.readNativeCommandFacts({
          threadId: ThreadId.make(input.identity.native_id),
          commandId: CommandId.make(attempt.nativeCommandId),
        });
        return yield* nativeProviderSettlementFromFacts(
          attempt,
          facts,
          expectedIdentities.get(attempt.nativeCommandId) ?? null,
        );
      }),
  };
};

export const nativeSettlementCommand = (input: NativeWorkstreamSettlementInputV2) => ({
  commandId: CommandId.make(input.attempt.nativeCommandId),
  threadId: ThreadId.make(input.request.identity.native_id),
  type:
    input.request.native_action === "settle"
      ? ("thread.settle" as const)
      : ("thread.unsettle" as const),
});
