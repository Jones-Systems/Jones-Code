import { ImportedHistoryStart, ImportedHistoryReview, ImportedHistoryOutcome, type ImportedHistoryDelivery, type ThreadId, type CommandId } from "@t3tools/contracts";
import { importedHistoryCanonicalJson } from "@t3tools/shared/jones/importedHistoryCanonical";
import * as Effect from "effect/Effect";
import * as Crypto from "effect/Crypto";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { Atom } from "effect/unstable/reactivity";
import type { HttpClient } from "effect/unstable/http";
import type { EnvironmentRegistry } from "../../connection/registry.ts";
import { EnvironmentSupervisor } from "../../connection/supervisor.ts";
import { RemoteEnvironmentAuthorization } from "../../authorization/service.ts";
import { ManagedRelayDpopSigner } from "../../relay/managedRelay.ts";
import { environmentEndpointUrl } from "../../environment/endpoint.ts";
import { executeAuthenticatedEnvironmentHttpRequest } from "../../state/environmentHttpAuth.ts";
import { createEnvironmentCommand } from "../../state/runtime.ts";
import { prepareImportedHistoryMessage, type StartThreadTurnInput } from "../../operations/commands.ts";

const request = Effect.fn("jonesImportedHistory.request")(function* (action:
  | { type: "review"; input: { threadId: ThreadId; delivery: ImportedHistoryDelivery } }
  | { type: "start"; input: ImportedHistoryStart }
  | { type: "observe"; input: { threadId: ThreadId; commandId: CommandId } },
) {
  const supervisor = yield* EnvironmentSupervisor;
  const prepared = yield* SubscriptionRef.get(supervisor.prepared);
  if (Option.isNone(prepared)) return yield* Effect.fail(new Error("Imported history connection is unavailable."));
  const signer = yield* Effect.serviceOption(ManagedRelayDpopSigner);
  const remoteAuthorization = yield* Effect.serviceOption(RemoteEnvironmentAuthorization);
  return yield* executeAuthenticatedEnvironmentHttpRequest({
    prepared: prepared.value, signer, remoteAuthorization,
    group: "jonesImportedHistory", method: "POST", timeoutMs: 30_000,
    url: (base) => environmentEndpointUrl(base, `/api/jones/imported-history/${action.type}`),
    request: ({ client, headers }) => {
      switch (action.type) {
        case "review": return client.review({ headers, payload: action.input }).pipe(Effect.map((value): unknown => value));
        case "start": return client.start({ headers, payload: action.input }).pipe(Effect.map((value): unknown => value));
        case "observe": return client.observe({ headers, payload: action.input }).pipe(Effect.map((value): unknown => value));
      }
    },
  });
});

export const importedHistoryIdentity = Effect.fn("jonesImportedHistory.identity")(function* (command: ImportedHistoryStart) {
  const encoded = yield* Schema.encodeEffect(ImportedHistoryStart)(command);
  const digest = (value: unknown) => Effect.tryPromise(async () => {
    const bytes = new TextEncoder().encode(importedHistoryCanonicalJson(value));
    const hash = await globalThis.crypto.subtle.digest("SHA-256", bytes);
    return Array.from(new Uint8Array(hash), (byte) => byte.toString(16).padStart(2, "0")).join("");
  });
  return { commandDigest: yield* digest(encoded), deliveryDigest: yield* digest(encoded.delivery) };
});

export function createImportedHistoryCommands<R, E>(runtime: Atom.AtomRuntime<EnvironmentRegistry | HttpClient.HttpClient | Crypto.Crypto | R, E>) {
  return {
    prepare: createEnvironmentCommand(runtime, { label: "jones:imported-history:prepare", execute: (input: StartThreadTurnInput) => prepareImportedHistoryMessage(input) }),
    review: createEnvironmentCommand(runtime, { label: "jones:imported-history:review", execute: (input: { threadId: ThreadId; delivery: ImportedHistoryDelivery }) => request({ type: "review", input }).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ImportedHistoryReview))) }),
    start: createEnvironmentCommand(runtime, { label: "jones:imported-history:start", execute: (input: ImportedHistoryStart) => request({ type: "start", input }).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ImportedHistoryOutcome))) }),
    observe: createEnvironmentCommand(runtime, { label: "jones:imported-history:observe", execute: (input: { threadId: ThreadId; commandId: CommandId }) => request({ type: "observe", input }).pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.NullOr(ImportedHistoryOutcome)))) }),
    identity: createEnvironmentCommand(runtime, { label: "jones:imported-history:identity", execute: importedHistoryIdentity }),
  };
}
