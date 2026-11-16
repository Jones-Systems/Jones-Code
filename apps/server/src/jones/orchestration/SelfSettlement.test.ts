import { assert, it } from "@effect/vitest";
import { CommandId, ProviderSessionId, ThreadPullRequestLink } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Fixture from "../threadResultDependencies/testkit.ts";
import { terminalDisposition } from "./SelfSettlement.ts";

it.effect("keeps checkpoint work separate and preserves upstream automatic-followup, user-work and watch guards", () =>
  Effect.gen(function* () {
    const f = yield* Fixture.seed;
    const original = { ...f.receiverRun, status: "completed" as const, completedAt: Fixture.time };
    const thread = { ...f.receiver, selfSettlement: { commandId: CommandId.make("self-intent"), runId: original.id,
      providerSessionId: ProviderSessionId.make("native-session"), mcpCredentialId: "mcp-credential",
      providerInstanceId: f.receiver.providerInstanceId } };
    const projection = { thread, runs: [original], messages: [], runtimeRequests: [] };
    const decide = (value: Parameters<typeof terminalDisposition>[0]["projection"] = projection) => terminalDisposition({ projection: value, acceptedAt: Fixture.time,
      duties: { outgoing: 0, owed: 0 } });
    assert.equal(decide(), "settle");
    assert.equal(decide({ ...projection, runs: [{ ...original, status: "waiting" }] }), "ignore");
    const following = f.run("automatic-following", f.receiver.id, "queued", 2);
    const wake = { ...f.message(following.userMessageId, following), notification: {
      source: { kind: "monitor" as const }, outcome: "completed" as const, summary: "Automatic wake" } };
    const withWake = { ...projection, runs: [original, following], messages: [wake] };
    assert.equal(decide(withWake), "settle");
    assert.equal(decide({ ...withWake, runs: [original, { ...following, status: "running" }] }), "ignore");
    assert.equal(decide({ ...withWake, runs: [original, { ...following, status: "completed" }] }), "settle");
    assert.equal(decide({ ...withWake, messages: [f.message(following.userMessageId, following)] }), "cancel");
    assert.equal(terminalDisposition({ projection, acceptedAt: Fixture.time, duties: { outgoing: 1, owed: 0 } }), "cancel");
    const watch = Schema.decodeUnknownSync(ThreadPullRequestLink)({ host: "github", repository: "owner/repo", number: 1,
      url: "https://github.com/owner/repo/pull/1", source: "manual", linkedAt: DateTime.formatIso(Fixture.time), snapshot: null,
      stack: null, watch: { startedAt: DateTime.formatIso(Fixture.time), headSha: null, failedChecks: [], passed: false,
        passedChecks: [], remarksThrough: DateTime.formatIso(Fixture.time), remarkIds: [], conflicting: false, wakes: 0 } });
    assert.equal(decide({ ...projection, thread: { ...thread, pullRequests: [watch] } }), "cancel");
  }).pipe(Effect.provide(Fixture.layer)),
);
