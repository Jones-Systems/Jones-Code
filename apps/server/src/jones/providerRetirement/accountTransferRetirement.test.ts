import { assert, it } from "@effect/vitest";
import {
  CommandId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ThreadId,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import type * as Outbox from "../../orchestration-v2/EffectOutbox.ts";
import * as Receipts from "../../orchestration-v2/CommandReceiptStore.ts";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import {
  ACCOUNT_CONTINUED,
  SELECTION_CHANGED,
  accountTransferDestination,
  decideAccountTransfer,
  isAccountTransferDetach,
  retirementConversationKey,
} from "./accountTransferRetirement.ts";

const source = ProviderInstanceId.make("account-A");
const target = ProviderInstanceId.make("account-B");
const threadId = ThreadId.make("transfer-thread");
const commandId = CommandId.make("transfer-command");
const metadata = {
  driver: ProviderDriverKind.make("codex"),
  enabled: true,
  continuationKey: "shared",
  capabilities: CodexProviderCapabilitiesV2,
};
const projection = {
  thread: { modelSelection: { instanceId: target }, archivedAt: null, deletedAt: null },
  runs: [],
} as unknown as OrchestrationV2ThreadProjection;
const facts = {
  optedIn: true,
  revokeMcpCredential: false,
  projection,
  sourceInstanceId: source,
  sourceDriver: "codex",
  supportsRetirement: true,
  attached: true,
  loadedKey: "captured-loaded-key",
  complete: true,
  sourceMetadata: metadata,
  destinationMetadata: metadata,
};

it.effect(
  "only accepted account-selection detaches opt in; explicit, revoking and unreadable detaches stay ordinary",
  () =>
    Effect.gen(function* () {
      const receipt = {
        commandId,
        threadId,
        commandType: "message.dispatch",
        status: "accepted" as const,
        acceptedAt: yield* DateTime.now,
        resultSequence: 1,
        error: null,
      };
      const effect = (detail: string, extra = {}): Outbox.OrchestrationEffectV2 =>
        ({
          commandId,
          threadId,
          request: {
            type: "provider-session.detach",
            providerSessionId: ProviderSessionId.make("A"),
            detail,
            ...extra,
          },
        }) as Outbox.OrchestrationEffectV2;
      for (const [detail, commandType] of [
        [SELECTION_CHANGED, "thread.model-selection.set"],
        [SELECTION_CHANGED, "provider.switch"],
        [ACCOUNT_CONTINUED, "message.dispatch"],
      ])
        assert.isTrue(
          yield* isAccountTransferDetach(effect(detail!), {
            getByCommandId: () =>
              Effect.succeedSome({ ...receipt, commandType: commandType! }),
          }),
        );
      for (const detail of [
        "Workspace changed.",
        "Runtime mode changed.",
        "Thread settled.",
        "Thread archived.",
      ]) {
        assert.isFalse(
          yield* isAccountTransferDetach(effect(detail), {
            getByCommandId: () => Effect.die("excluded reasons must not read receipts"),
          }),
        );
      }
      for (const changed of [
        { commandType: "provider-session.detach" },
        { status: "rejected" as const },
        { threadId: ThreadId.make("other") },
      ]) {
        assert.isFalse(
          yield* isAccountTransferDetach(effect(SELECTION_CHANGED), {
            getByCommandId: () => Effect.succeedSome({ ...receipt, ...changed }),
          }),
        );
      }
      for (const extra of [{ revokeMcpCredential: true }, { runtimeStopCommandId: commandId }]) {
        assert.isFalse(
          yield* isAccountTransferDetach(effect(ACCOUNT_CONTINUED, extra), {
            getByCommandId: () => Effect.die("excluded requests must not read receipts"),
          }),
        );
      }
      assert.isFalse(
        yield* isAccountTransferDetach(effect(SELECTION_CHANGED), {
          getByCommandId: () => Effect.succeedNone,
        }),
      );
      assert.isFalse(
        yield* isAccountTransferDetach(effect(SELECTION_CHANGED), {
          getByCommandId: () =>
            Effect.fail(new Receipts.CommandReceiptStoreReadError({ commandId })),
        }),
      );
    }),
);

it("only a loaded complete source with a compatible different next Codex account requires proof", () => {
  assert.deepEqual(decideAccountTransfer(facts), {
    type: "prove",
    loadedKey: facts.loadedKey,
    conversationKey: JSON.stringify(["codex", "shared"]),
  });
  const exclusions: Array<Partial<typeof facts>> = [
    { optedIn: false },
    { revokeMcpCredential: true },
    { attached: false },
    { complete: false },
    { sourceDriver: "claudeAgent" },
    { supportsRetirement: false },
    { destinationMetadata: { ...metadata, enabled: false } },
    { destinationMetadata: { ...metadata, continuationKey: "different" } },
    { sourceMetadata: { ...metadata, continuationKey: " " } },
    {
      projection: {
        ...projection,
        thread: { ...projection.thread, archivedAt: DateTime.makeUnsafe(0) },
      },
    },
    {
      projection: {
        ...projection,
        thread: { ...projection.thread, deletedAt: DateTime.makeUnsafe(0) },
      },
    },
    {
      projection: {
        ...projection,
        thread: {
          ...projection.thread,
          modelSelection: { instanceId: source, model: "synthetic" },
        },
      },
    },
  ];
  for (const excluded of exclusions)
    assert.equal(decideAccountTransfer({ ...facts, ...excluded }).type, "ordinary");
  assert.equal(decideAccountTransfer({ ...facts, loadedKey: undefined }).type, "ordinary");
  assert.equal(decideAccountTransfer({ ...facts, projection: undefined }).type, "ordinary");
  assert.equal(decideAccountTransfer({ ...facts, sourceMetadata: undefined }).type, "ordinary");
  assert.equal(
    decideAccountTransfer({ ...facts, destinationMetadata: undefined }).type,
    "ordinary",
  );
});

it("next admission uses an unstarted starting run, then saved defaults; queued, preparing and auth-started runs do not name the writer", () => {
  const run = (status: string, startedAt: DateTime.Utc | null = null) =>
    ({
      status,
      startedAt,
      modelSelection: { instanceId: source },
    }) as OrchestrationV2ThreadProjection["runs"][number];
  for (const runs of [
    [run("queued")],
    [run("preparing")],
    [run("starting", DateTime.makeUnsafe(0))],
  ]) {
    assert.equal(accountTransferDestination({ ...projection, runs }), target);
    assert.equal(
      decideAccountTransfer({ ...facts, projection: { ...projection, runs } }).type,
      "prove",
    );
  }
  const starting = { ...projection, runs: [run("starting")] };
  assert.equal(accountTransferDestination(starting), source);
  assert.equal(decideAccountTransfer({ ...facts, projection: starting }).type, "ordinary");
  const unrelated = retirementConversationKey({
    ...metadata,
    driver: ProviderDriverKind.make("claudeAgent"),
    continuationKey: "",
  });
  assert.isDefined(unrelated);
  assert.notEqual(unrelated, retirementConversationKey(metadata));
});
