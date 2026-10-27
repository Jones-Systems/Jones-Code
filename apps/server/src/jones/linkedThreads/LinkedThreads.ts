import { MessageId, type ThreadId } from "@t3tools/contracts";
import {
  LinkedThreadsError,
  type ThreadEndpointRef,
  type LinkedThreadId,
  LinkedThreadHandoff,
  type LinkedThreadMutationResult,
  type LinkedThreadsListInput,
  type LinkedThreadsListResult,
  type LinkThreadsInput,
  type UnlinkThreadsInput,
  type AcceptThreadHandoffInput,
  type LinkedThreadOriginLink,
} from "@t3tools/contracts/jones/linkedThreads";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Environment from "../../environment/ServerEnvironment.ts";
import * as Threads from "../../orchestration-v2/ThreadManagementService.ts";
import { authorize, assertedBy, type LinkActor } from "./authorization.ts";
import { linkedThreadIdentity, sameEndpoint } from "./identity.ts";
import * as Repository from "./repository.ts";

const encodeEndpointKey = Schema.encodeSync(
  Schema.fromJsonString(Schema.Tuple([Schema.String, Schema.String])),
);
const encodeHandoff = Schema.encodeSync(Schema.fromJsonString(LinkedThreadHandoff));
const isLinkedThreadsError = Schema.is(LinkedThreadsError);
type ServiceRequirements = SqlClient.SqlClient | Threads.ThreadManagementService;

type LinkInput = LinkThreadsInput & {
  readonly basis: "manual" | "handoff";
  readonly handoff?: LinkedThreadHandoff;
};
type Claim = {
  readonly from: ThreadEndpointRef;
  readonly toThreadId: ThreadId;
  readonly launchCommandId: string;
  readonly actor: LinkActor;
  readonly remote?: { readonly environmentLabel: string | null; readonly title: string | null };
};
export type RecoverResult = {
  readonly outcome: "recorded" | "already_recorded" | "previously_removed" | "no_exact_evidence";
  readonly link: LinkedThreadMutationResult["link"] | null;
  readonly evidence?: "launch_first_message" | "created_thread_record";
};
export class LinkedThreads extends Context.Service<
  LinkedThreads,
  {
    readonly claimLaunch: (input: Claim) => Effect.Effect<LinkedThreadId, LinkedThreadsError>;
    readonly recordRemoteLaunch: (input: {
      readonly from: ThreadEndpointRef;
      readonly to: ThreadEndpointRef;
      readonly reported?: LinkedThreadOriginLink;
      readonly title: string | null;
      readonly environmentLabel: string | null;
      readonly actor: LinkActor;
    }) => Effect.Effect<LinkedThreadOriginLink>;
    readonly list: (
      actor: LinkActor,
      input: LinkedThreadsListInput,
    ) => Effect.Effect<LinkedThreadsListResult, LinkedThreadsError>;
    readonly link: (
      actor: LinkActor,
      input: LinkInput,
    ) => Effect.Effect<LinkedThreadMutationResult, LinkedThreadsError>;
    readonly unlink: (
      actor: LinkActor,
      input: UnlinkThreadsInput,
    ) => Effect.Effect<LinkedThreadMutationResult, LinkedThreadsError>;
    readonly accept: (
      actor: LinkActor,
      input: AcceptThreadHandoffInput,
    ) => Effect.Effect<LinkedThreadMutationResult, LinkedThreadsError>;
    readonly recover: (
      actor: LinkActor,
      input: { readonly threadId: ThreadId },
    ) => Effect.Effect<RecoverResult, LinkedThreadsError>;
  }
>()("t3/jones/linkedThreads/LinkedThreads") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const threads = yield* Threads.ThreadManagementService;
  const environment = yield* Environment.ServerEnvironment;
  const here = yield* environment.getEnvironmentId;
  const now = DateTime.now.pipe(Effect.map(DateTime.formatIso));
  const project = (row: Repository.LinkRow) => Repository.project(row, here);
  const required = Effect.fn("LinkedThreads.required")(function* (id: string) {
    const row = yield* Repository.read(id);
    if (row === undefined)
      return yield* new LinkedThreadsError({
        reason: "not_found",
        message: "The thread link was not found.",
      });
    return row;
  });
  const rowFor = (
    kind: Repository.LinkRow["kind"],
    from: ThreadEndpointRef,
    to: ThreadEndpointRef,
    basis: Repository.LinkRow["basis"],
    actor: LinkActor,
    time: string,
  ): Repository.LinkRow => {
    const identity = linkedThreadIdentity(kind, from, to);
    return {
      link_id: identity.linkId,
      kind,
      from_environment_id: identity.from.environmentId,
      from_thread_id: identity.from.threadId,
      to_environment_id: identity.to.environmentId,
      to_thread_id: identity.to.threadId,
      basis,
      asserted_by: assertedBy(actor),
      launch_command_id: null,
      from_title:
        actor.type === "linked" &&
        actor.origin !== null &&
        sameEndpoint(actor.origin, identity.from)
          ? (actor.caller?.scope.client?.remoteOrigin?.thread?.title?.slice(0, 512) ?? null)
          : null,
      to_title:
        actor.type === "linked" && actor.origin !== null && sameEndpoint(actor.origin, identity.to)
          ? (actor.caller?.scope.client?.remoteOrigin?.thread?.title?.slice(0, 512) ?? null)
          : null,
      remote_environment_label: actor.type === "linked" ? actor.label.slice(0, 256) : null,
      reverse_state: from.environmentId === to.environmentId ? "local" : "unknown",
      acceptance: kind === "continuation" ? "offered" : null,
      handoff_json: null,
      created_at: time,
      updated_at: time,
      accepted_at: null,
      removed_at: null,
    };
  };
  const validate = Effect.fn("LinkedThreads.validate")(function* (
    actor: LinkActor,
    from: ThreadEndpointRef,
    to: ThreadEndpointRef,
    claimTo?: ThreadId,
  ) {
    if (sameEndpoint(from, to))
      return yield* new LinkedThreadsError({
        reason: "self_link",
        message: "A thread cannot link to itself.",
      });
    yield* authorize(actor, here, [from, to], claimTo === undefined ? {} : { claimTo });
  });
  const checkCycle = Effect.fn("LinkedThreads.checkCycle")(function* (
    from: ThreadEndpointRef,
    to: ThreadEndpointRef,
  ) {
    const queue = [to];
    const seen = new Set<string>();
    while (queue.length > 0) {
      const endpoint = queue.shift()!;
      if (sameEndpoint(endpoint, from))
        return yield* new LinkedThreadsError({
          reason: "cycle",
          message: "This continuation would create a cycle.",
        });
      const key = encodeEndpointKey([endpoint.environmentId, endpoint.threadId]);
      if (seen.has(key)) continue;
      if (seen.size >= 64)
        return yield* new LinkedThreadsError({
          reason: "cycle",
          message: "The continuation path exceeds the local cycle-check limit.",
        });
      seen.add(key);
      for (const row of yield* Repository.outgoing(here, endpoint, "continuation", 65)) {
        if (row.kind !== "continuation") continue;
        const link = project(row);
        if (sameEndpoint(link.from, endpoint)) queue.push(link.to);
      }
    }
  });
  const claimLaunch = Effect.fn("LinkedThreads.claimLaunch")(function* (
    input: Claim,
  ): Effect.fn.Return<LinkedThreadId, LinkedThreadsError, ServiceRequirements> {
    const to = { environmentId: here, threadId: input.toThreadId };
    // The destination does not exist yet. The launch's preflight owns its mode and fence checks.
    return yield* sql
      .withTransaction(
        Effect.gen(function* () {
          yield* validate(input.actor, input.from, to, input.toThreadId);
          const row = rowFor("launch", input.from, to, "launch", input.actor, yield* now);
          yield* Repository.insert({
            ...row,
            launch_command_id: input.launchCommandId,
            from_title: input.remote?.title?.slice(0, 512) ?? null,
            remote_environment_label: input.remote?.environmentLabel?.slice(0, 256) ?? null,
          });
          return row.link_id;
        }),
      )
      .pipe(
        Effect.mapError((error) =>
          isLinkedThreadsError(error) ? error : Repository.storageFailure(),
        ),
      );
  });
  const list = Effect.fn("LinkedThreads.list")(function* (
    actor: LinkActor,
    input: LinkedThreadsListInput,
  ): Effect.fn.Return<LinkedThreadsListResult, LinkedThreadsError, ServiceRequirements> {
    if (actor.type === "linked" && actor.origin === null)
      return yield* new LinkedThreadsError({
        reason: "forbidden",
        message: "A linked caller needs a bound origin thread.",
      });
    // A remote subject is permitted only when this environment holds a touching local copy.
    if (input.thread.environmentId === here)
      yield* authorize(actor, here, [input.thread], { read: true });
    const limit = input.limit ?? 100;
    const rows = yield* Repository.touching(here, input.thread, limit + 1);
    if (input.thread.environmentId !== here) {
      if (rows.length === 0)
        return { environmentId: here, links: [], siblings: [], truncated: false };
      for (const row of rows) {
        const link = project(row);
        yield* authorize(actor, here, [link.from, link.to], { read: true });
      }
    }
    const links = rows.slice(0, limit).map(project);
    const siblings: LinkedThreadsListResult["siblings"][number][] = [];
    let truncated = rows.length > limit;
    if (input.includeSiblings !== false) {
      const ids = new Set<string>();
      for (const link of links) {
        if (link.kind !== "launch" || !sameEndpoint(link.to, input.thread)) continue;
        for (const siblingRow of yield* Repository.outgoing(here, link.from, "launch", limit + 2)) {
          const sibling = project(siblingRow);
          if (
            sibling.kind !== "launch" ||
            !sameEndpoint(sibling.from, link.from) ||
            sameEndpoint(sibling.to, input.thread) ||
            ids.has(sibling.linkId)
          )
            continue;
          if (siblings.length >= limit) {
            truncated = true;
            break;
          }
          ids.add(sibling.linkId);
          siblings.push({ via: link.from, link: sibling });
        }
      }
    }
    return { environmentId: here, links, siblings, truncated };
  });
  const link = Effect.fn("LinkedThreads.link")(function* (
    actor: LinkActor,
    input: LinkInput,
  ): Effect.fn.Return<LinkedThreadMutationResult, LinkedThreadsError, ServiceRequirements> {
    const encoded = input.handoff === undefined ? null : encodeHandoff(input.handoff);
    if (encoded !== null && new TextEncoder().encode(encoded).byteLength > 65536)
      return yield* new LinkedThreadsError({
        reason: "too_large",
        message: "The handoff is too large.",
      });
    return yield* sql
      .withTransaction(
        Effect.gen(function* () {
          yield* validate(actor, input.from, input.to);
          const row = rowFor(input.kind, input.from, input.to, input.basis, actor, yield* now);
          const existing = yield* Repository.read(row.link_id);
          if (input.basis === "handoff" && existing !== undefined && existing.removed_at !== null)
            return yield* new LinkedThreadsError({
              reason: "not_found",
              message: "This continuation was unlinked and cannot be revived by a handoff retry.",
            });
          if (
            input.kind === "continuation" &&
            (existing === undefined || existing.removed_at !== null)
          )
            yield* checkCycle(input.from, input.to);
          const reverse =
            row.reverse_state === "local"
              ? "local"
              : (input.reverse ?? (actor.type === "linked" ? "recorded" : "unknown"));
          const snapshot = input.remote;
          yield* Repository.insert({
            ...row,
            reverse_state: reverse,
            handoff_json: encoded,
            remote_environment_label:
              snapshot?.environmentLabel ?? (actor.type === "linked" ? actor.label : null),
            from_title: row.from_environment_id !== here ? (snapshot?.title ?? null) : null,
            to_title: row.to_environment_id !== here ? (snapshot?.title ?? null) : null,
          });
          // Explicit link is the sole operation that may revive a continuation or peer tombstone.
          yield* sql`UPDATE jones_thread_links SET removed_at = NULL, reverse_state = ${reverse},
        handoff_json = COALESCE(${encoded}, handoff_json),
        remote_environment_label = COALESCE(${snapshot?.environmentLabel ?? null}, remote_environment_label),
        from_title = COALESCE(${row.from_environment_id !== here ? (snapshot?.title ?? null) : null}, from_title),
        to_title = COALESCE(${row.to_environment_id !== here ? (snapshot?.title ?? null) : null}, to_title), updated_at = ${yield* now}
        WHERE link_id = ${row.link_id}`.pipe(Effect.mapError(Repository.storageFailure));
          return {
            link: project(yield* required(row.link_id)),
            outcome:
              existing === undefined || existing.removed_at !== null ? "recorded" : "unchanged",
          } satisfies LinkedThreadMutationResult;
        }),
      )
      .pipe(
        Effect.mapError((error) =>
          isLinkedThreadsError(error) ? error : Repository.storageFailure(),
        ),
      );
  });
  const unlink = Effect.fn("LinkedThreads.unlink")(function* (
    actor: LinkActor,
    input: UnlinkThreadsInput,
  ): Effect.fn.Return<LinkedThreadMutationResult, LinkedThreadsError, ServiceRequirements> {
    const row = yield* required(input.linkId);
    const value = project(row);
    if (!(actor.type === "thread" && row.asserted_by === `thread:${actor.threadId}`))
      yield* authorize(actor, here, [value.from, value.to]);
    yield* sql`UPDATE jones_thread_links SET removed_at = COALESCE(removed_at, ${yield* now}), updated_at = ${yield* now}, reverse_state = ${value.reverse === "local" ? "local" : (input.reverse ?? value.reverse)} WHERE link_id = ${input.linkId}`.pipe(
      Effect.mapError(Repository.storageFailure),
    );
    return { link: project(yield* required(input.linkId)), outcome: "removed" };
  });
  const accept = Effect.fn("LinkedThreads.accept")(function* (
    actor: LinkActor,
    input: AcceptThreadHandoffInput,
  ): Effect.fn.Return<LinkedThreadMutationResult, LinkedThreadsError, ServiceRequirements> {
    const receiver =
      actor.type === "user"
        ? input.to.environmentId === here
        : actor.type === "thread"
          ? input.to.environmentId === here && input.to.threadId === actor.threadId
          : actor.type === "linked" &&
            actor.origin !== null &&
            sameEndpoint(actor.origin, input.to);
    if (!receiver)
      return yield* new LinkedThreadsError({
        reason: "not_receiver",
        message: "Only the receiving thread can accept this handoff.",
      });
    const id = linkedThreadIdentity("continuation", input.from, input.to).linkId;
    if (input.linkId !== undefined && input.linkId !== id)
      return yield* new LinkedThreadsError({
        reason: "not_found",
        message: "The link does not match these continuation endpoints.",
      });
    return yield* sql
      .withTransaction(
        Effect.gen(function* () {
          yield* validate(actor, input.from, input.to);
          const existing = yield* Repository.read(id);
          if (existing?.removed_at !== null && existing !== undefined)
            return yield* new LinkedThreadsError({
              reason: "not_found",
              message: "This handoff was unlinked; explicitly link it before accepting.",
            });
          if (existing === undefined) {
            yield* checkCycle(input.from, input.to);
            yield* Repository.insert(
              rowFor("continuation", input.from, input.to, "handoff", actor, yield* now),
            );
          }
          const nextReverse = input.reverse ?? (actor.type === "linked" ? "recorded" : "unknown");
          yield* sql`UPDATE jones_thread_links SET acceptance = 'accepted', accepted_at = COALESCE(accepted_at, ${yield* now}), updated_at = ${yield* now},
        reverse_state = CASE
          WHEN ${input.from.environmentId === input.to.environmentId ? 1 : 0} THEN 'local'
          WHEN ${nextReverse} = 'recorded' THEN 'recorded'
          WHEN reverse_state = 'unknown' THEN ${nextReverse}
          ELSE reverse_state
        END WHERE link_id = ${id}`.pipe(Effect.mapError(Repository.storageFailure));
          return {
            link: project(yield* required(id)),
            outcome: "accepted",
          } satisfies LinkedThreadMutationResult;
        }),
      )
      .pipe(
        Effect.mapError((error) =>
          isLinkedThreadsError(error) ? error : Repository.storageFailure(),
        ),
      );
  });
  const recordRemoteLaunch = Effect.fn("LinkedThreads.recordRemoteLaunch")(function* (
    input: Parameters<LinkedThreads["Service"]["recordRemoteLaunch"]>[0],
  ): Effect.fn.Return<LinkedThreadOriginLink, never, ServiceRequirements> {
    const row = rowFor("launch", input.from, input.to, "launch", input.actor, yield* now);
    const reverse: Repository.LinkRow["reverse_state"] =
      input.reported?.linkId === row.link_id && input.reported.state === "recorded"
        ? "recorded"
        : "unavailable";
    return yield* Effect.gen(function* () {
      yield* validate(input.actor, input.from, input.to);
      yield* Repository.insert({
        ...row,
        reverse_state: reverse,
        to_title: input.title?.slice(0, 512) ?? null,
        remote_environment_label: input.environmentLabel?.slice(0, 256) ?? null,
      });
      yield* sql`UPDATE jones_thread_links SET reverse_state = ${reverse}, updated_at = ${yield* now} WHERE link_id = ${row.link_id}`.pipe(
        Effect.mapError(Repository.storageFailure),
      );
      return { linkId: row.link_id, state: "recorded" as const, reverse };
    }).pipe(
      Effect.catch(() =>
        Effect.logWarning("Forwarded launch navigation copy was not recorded").pipe(
          Effect.as({ linkId: row.link_id, state: "not_recorded" as const, reverse }),
        ),
      ),
    );
  });
  const recover = Effect.fn("LinkedThreads.recover")(function* (
    actor: LinkActor,
    input: { readonly threadId: ThreadId },
  ): Effect.fn.Return<RecoverResult, LinkedThreadsError, ServiceRequirements> {
    if (actor.type === "linked")
      return yield* new LinkedThreadsError({
        reason: "forbidden",
        message: "Linked callers cannot recover local launch evidence.",
      });
    yield* authorize(actor, here, [{ environmentId: here, threadId: input.threadId }]);
    const projection = yield* threads
      .getThreadProjection(input.threadId)
      .pipe(Effect.mapError(Repository.storageFailure));
    if (
      projection.thread.createdBy !== "agent" ||
      projection.thread.creationSource !== "mcp" ||
      projection.thread.delegatedFrom !== undefined ||
      projection.thread.lineage?.parentThreadId != null ||
      projection.thread.forkedFrom !== null
    )
      return { outcome: "no_exact_evidence", link: null };
    const messages = projection.messages.filter((item) => item.role === "user");
    const exact = messages.find(
      (item) => item.id === MessageId.make(input.threadId) && item.senderThreadId !== undefined,
    );
    const first = messages[0];
    const sender = exact?.senderThreadId ?? first?.senderThreadId;
    if (sender === undefined) return { outcome: "no_exact_evidence", link: null };
    const source = yield* threads
      .getThreadShell(sender)
      .pipe(Effect.mapError(Repository.storageFailure));
    if (source === null || source.deletedAt !== null)
      return { outcome: "no_exact_evidence", link: null };
    let evidence: "launch_first_message" | "created_thread_record" = "launch_first_message";
    if (exact === undefined) {
      const origin = yield* threads
        .getThreadProjection(sender)
        .pipe(Effect.mapError(Repository.storageFailure));
      if (
        !origin.turnItems.some(
          (record) => record.type === "thread_created" && record.targetThreadId === input.threadId,
        )
      )
        return { outcome: "no_exact_evidence", link: null };
      evidence = "created_thread_record";
    }
    const row = {
      ...rowFor(
        "launch",
        { environmentId: here, threadId: sender },
        { environmentId: here, threadId: input.threadId },
        "recovered",
        actor,
        yield* now,
      ),
      asserted_by: "system",
    };
    const existing = yield* Repository.read(row.link_id);
    if (existing !== undefined)
      return {
        outcome: existing.removed_at === null ? "already_recorded" : "previously_removed",
        link: project(existing),
      };
    yield* Repository.insert(row);
    return { outcome: "recorded", link: project(yield* required(row.link_id)), evidence };
  });
  const provide = <A, E>(effect: Effect.Effect<A, E, ServiceRequirements>) =>
    effect.pipe(
      Effect.provideService(SqlClient.SqlClient, sql),
      Effect.provideService(Threads.ThreadManagementService, threads),
    );
  return LinkedThreads.of({
    claimLaunch: (input: Claim) => provide(claimLaunch(input)),
    recordRemoteLaunch: (input: Parameters<LinkedThreads["Service"]["recordRemoteLaunch"]>[0]) =>
      provide(recordRemoteLaunch(input)),
    list: (actor: LinkActor, input: LinkedThreadsListInput) => provide(list(actor, input)),
    link: (actor: LinkActor, input: LinkInput) => provide(link(actor, input)),
    unlink: (actor: LinkActor, input: UnlinkThreadsInput) => provide(unlink(actor, input)),
    accept: (actor: LinkActor, input: AcceptThreadHandoffInput) => provide(accept(actor, input)),
    recover: (actor: LinkActor, input: { readonly threadId: ThreadId }) =>
      provide(recover(actor, input)),
  });
});
export const layer = Layer.effect(LinkedThreads, make);
