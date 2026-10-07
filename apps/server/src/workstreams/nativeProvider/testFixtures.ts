import conformance from "../../../../../packages/contracts/contracts/workstreams-t3-provider/v1/fixtures/conformance.json" with { type: "json" };
import {
  WorkstreamsNativeContext,
  WorkstreamsNativeSettlementRequest,
  WorkstreamsNativeAttestationRequest,
  WorkstreamsNativeTerminalResult,
  CommandId,
  EventId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { type NativeProviderAttempt, type NativeProviderAttempts } from "./attemptRepository.ts";
import { NATIVE_PROVIDER_SCOPES, type NativeProviderEnrollmentBinding } from "./enrollment.ts";
import { makeWorkstreamsNativeProvider, sha256Bytes, type NativeProviderPorts } from "./service.ts";
import type { OrchestrationCommandReceipt } from "../../persistence/Services/OrchestrationCommandReceipts.ts";
import type { NativeCommandEventMetadataRow } from "../../persistence/Services/NativeCommandEventMetadata.ts";

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
  const receipts = new Map<string, OrchestrationCommandReceipt>();
  const events = new Map<string, ReadonlyArray<NativeCommandEventMetadataRow>>();
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
  const commit = (
    commandId: string,
    action: "settle" | "unsettle" = "settle",
    companions = false,
  ) => {
    receipts.set(commandId, {
      commandId: CommandId.make(commandId),
      aggregateKind: "thread",
      aggregateId: ThreadId.make(request.identity.native_id),
      commandType: action === "settle" ? "thread.settle" : "thread.unsettle",
      acceptedAt: now,
      resultSequence: companions ? 2 : 1,
      status: "accepted",
      error: null,
    });
    events.set(commandId, [
      {
        eventId: EventId.make("event-synthetic"),
        commandId: CommandId.make(commandId),
        aggregateKind: "thread",
        aggregateId: ThreadId.make(request.identity.native_id),
        sequence: 1,
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
              sequence: 2,
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
    evidence: {
      readSnapshotByCommandId: (commandId) =>
        Effect.sync(() => {
          const receipt = receipts.get(commandId);
          return {
            receipt:
              receipt === undefined
                ? Option.none()
                : Option.some({
                    commandId: receipt.commandId,
                    aggregateKind: receipt.aggregateKind,
                    aggregateId: receipt.aggregateId,
                    commandType: receipt.commandType,
                    acceptedAt: receipt.acceptedAt,
                    resultSequence: receipt.resultSequence,
                    status: receipt.status,
                  }),
            events: events.get(commandId) ?? [],
          };
        }),
    },
    build: Effect.succeed(Option.some(binding.build)),
    engine: {
      dispatch: (command) =>
        Effect.sync(() => {
          if (command.type !== "thread.settle" && command.type !== "thread.unsettle")
            throw new Error("Unexpected native command.");
          const attempt = attempts.get(key(request));
          if (!attempt || attempt.dispatchStartedAt === null)
            throw new Error("Dispatch preceded durable start.");
          calls.push({
            commandId: command.commandId,
            threadId: command.threadId,
            action: command.type,
          });
          commit(command.commandId, command.type === "thread.settle" ? "settle" : "unsettle");
          return { sequence: 1, storedEvents: [] };
        }),
    },
  };
  return {
    attempts,
    receipts,
    events,
    calls,
    ports,
    key,
    commit,
    provider: () => makeWorkstreamsNativeProvider(ports),
  };
};
