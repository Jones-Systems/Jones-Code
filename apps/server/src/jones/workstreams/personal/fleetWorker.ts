import type { EnvironmentId, OrchestratorMcpFailure } from "@t3tools/contracts";
import type * as P from "@t3tools/contracts/jones/workstreamsPersonal";
import type { ManagePersonalWorkstreamFleetInput } from "@t3tools/contracts/jones/workstreamsPersonalRpc";
import {
  canonicalPersonalWorkstream,
  WorkstreamsPersonalError,
} from "@t3tools/contracts/jones/workstreamsPersonal";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schedule from "effect/Schedule";
import * as PeerLinks from "../../../peer/PeerLinks.ts";
import * as PeerMcpClient from "../../../peer/PeerMcpClient.ts";
import * as Protocol from "./fleetProtocol.ts";
import type {
  StoredWorkstreamOperation,
  WorkstreamDeliveryRow,
  WorkstreamFleetMember,
} from "./fleetStore.ts";
import { PersonalWorkstreams } from "./service.ts";

export class WorkstreamFleetWorker extends Context.Service<
  WorkstreamFleetWorker,
  {
    readonly manage: (
      input: ManagePersonalWorkstreamFleetInput,
    ) => Effect.Effect<P.PersonalWorkstreamFleet, WorkstreamsPersonalError>;
    readonly reconcile: Effect.Effect<void, WorkstreamsPersonalError>;
  }
>()("t3/jones/workstreams/personal/fleetWorker/WorkstreamFleetWorker") {}

const limits: PeerMcpClient.PeerCallLimits = {
  runtimeMode: "approval-required",
  interactionMode: "default",
};
const peerFailure = (error: OrchestratorMcpFailure) =>
  error.code === "capability_denied"
    ? "peer_access_required"
    : error.code === "invalid_request"
      ? "peer_update_required"
      : "peer_unavailable";

export const make = Effect.gen(function* () {
  const workstreams = yield* PersonalWorkstreams;
  const links = yield* PeerLinks.PeerLinks;
  const peers = yield* PeerMcpClient.PeerMcpClient;
  const getGeneration = (id: EnvironmentId) =>
    links
      .currentGeneration(id)
      .pipe(Effect.mapError(() => new WorkstreamsPersonalError({ code: "storage_unavailable" })));
  const requireMember = Effect.fn("WorkstreamFleet.requireMember")(function* (
    environmentId: EnvironmentId,
  ) {
    const generation = yield* getGeneration(environmentId);
    if (Option.isNone(generation))
      return yield* new WorkstreamsPersonalError({ code: "precondition_failed" });
    return { environmentId, linkGeneration: generation.value };
  });
  const manage = Effect.fn("WorkstreamFleet.manage")(function* (
    input: ManagePersonalWorkstreamFleetInput,
  ) {
    if (input.kind === "designate") {
      const members = yield* Effect.forEach(new Set(input.peerEnvironmentIds), requireMember);
      return yield* workstreams.designateFleet(members);
    }
    if (input.kind === "follow")
      return yield* workstreams.approveFollower(input.authorityEnvironmentId, input.sessionId);
    if (input.kind === "remove")
      return yield* workstreams.changeRoster(
        input.environmentId,
        input.expectedRosterRevision,
        null,
      );
    const member = yield* requireMember(input.environmentId);
    return yield* workstreams.changeRoster(
      input.environmentId,
      input.expectedRosterRevision,
      member.linkGeneration,
    );
  });
  const checkPeer = Effect.fn("WorkstreamFleet.checkPeer")(function* (
    authorityId: EnvironmentId,
    member: WorkstreamFleetMember,
  ) {
    const generation = yield* getGeneration(member.environmentId);
    if (Option.isNone(generation)) return "peer_link_required";
    if (generation.value !== member.linkGeneration) return "peer_link_changed";
    const link = yield* links.resolve(member.environmentId).pipe(Effect.result);
    if (link._tag === "Failure") {
      if (link.failure.reason === "expired") return "peer_link_expired";
      return link.failure.reason === "incompatible" ? "peer_update_required" : "peer_unavailable";
    }
    if (link.success.generation !== member.linkGeneration) return "peer_link_changed";
    if (link.success.link.access === "read-only") return "peer_access_required";
    if (link.success.capabilities.workstreamsPersonalFleet?.version !== 1)
      return "peer_update_required";
    return yield* peers
      .call({
        environmentId: member.environmentId,
        expectedGeneration: member.linkGeneration,
        tool: "t3_workstream_fleet_enroll",
        arguments: { authorityEnvironmentId: authorityId },
        success: Protocol.FleetPeerEnrollment,
        limits,
      })
      .pipe(
        Effect.match({
          onFailure: peerFailure,
          onSuccess: (response) => {
            if (response.environmentId !== member.environmentId) return "peer_identity_changed";
            return response.state === "approved" ? null : response.state;
          },
        }),
      );
  });
  const readLeg = Effect.fn("WorkstreamFleet.readLeg")(function* (
    authorityId: EnvironmentId,
    operation: StoredWorkstreamOperation,
    leg: WorkstreamDeliveryRow,
    blockedBy: string | null,
  ) {
    if (blockedBy) {
      yield* workstreams.recordDelivery(leg, "blocked", blockedBy);
      return { kind: "blocked" } as const;
    }
    const response = yield* peers
      .call({
        environmentId: leg.environmentId,
        expectedGeneration: leg.linkGeneration,
        tool: "t3_workstream_definition_read",
        arguments: { authorityEnvironmentId: authorityId, workstreamId: operation.workstreamId },
        success: Protocol.FleetPeerDefinition,
        limits,
      })
      .pipe(Effect.result);
    if (response._tag === "Failure") {
      yield* workstreams.recordDelivery(leg, "blocked", peerFailure(response.failure));
      return { kind: "blocked" } as const;
    }
    const { workstream, lastRequestId, lastOrdinal } = response.success;
    if (operation.kind === "recovery") {
      const partialClose =
        workstream !== null &&
        operation.base !== null &&
        canonicalPersonalWorkstream(workstream) === canonicalPersonalWorkstream(operation.base);
      if (lastOrdinal !== null && lastOrdinal >= operation.ordinal && !partialClose) {
        yield* workstreams.recordDelivery(leg, "delivered", null);
        return { kind: "delivered" } as const;
      }
      // Fence the older in-flight request even if the receiver still has an
      // unchanged or missing definition. Only its exact partial close is restored.
      return {
        kind: "ready",
        leg,
        expected: workstream,
        candidate: partialClose ? operation.candidate : workstream,
      } as const;
    }
    if (
      lastRequestId === operation.requestId &&
      workstream !== null &&
      canonicalPersonalWorkstream(workstream) === canonicalPersonalWorkstream(operation.candidate)
    ) {
      yield* workstreams.recordDelivery(leg, "delivered", null);
      return { kind: "delivered" } as const;
    }
    if (!(yield* workstreams.knownPeerBase(operation, leg, workstream))) {
      yield* workstreams.recordDelivery(leg, "conflict", "different_definition", workstream);
      return { kind: "blocked" } as const;
    }
    return { kind: "ready", leg, expected: workstream, candidate: operation.candidate } as const;
  });
  const deliver = Effect.fn("WorkstreamFleet.deliver")(function* (
    authorityId: EnvironmentId,
    operation: StoredWorkstreamOperation,
    {
      leg,
      expected,
      candidate,
    }: {
      leg: WorkstreamDeliveryRow;
      expected: P.PersonalWorkstream | null;
      candidate: P.PersonalWorkstream | null;
    },
  ) {
    if (!(yield* workstreams.beginDelivery(leg))) return false;
    const result = yield* peers
      .call({
        environmentId: leg.environmentId,
        expectedGeneration: leg.linkGeneration,
        tool: "t3_workstream_definition_apply",
        arguments: {
          authorityEnvironmentId: authorityId,
          requestId: operation.requestId,
          workstreamId: operation.workstreamId,
          ordinal: operation.ordinal,
          closing: operation.kind === "close",
          candidate,
          expected,
        } satisfies Protocol.FleetPeerApplyDefinition,
        success: Protocol.FleetPeerApplyResult,
        limits,
      })
      .pipe(Effect.result);
    if (result._tag === "Failure") {
      yield* workstreams.recordDelivery(leg, "unknown", peerFailure(result.failure));
      return false;
    }
    yield* workstreams.recordDelivery(
      leg,
      result.success.state,
      result.success.reason,
      result.success.state === "conflict" ? expected : null,
    );
    return result.success.state === "delivered";
  });
  const reconcile = Effect.gen(function* () {
    const fleet = yield* workstreams.currentFleetMembers;
    if (!fleet) return;
    const operations = yield* workstreams.pendingOperations;
    const peerKey = ({ environmentId, linkGeneration }: WorkstreamFleetMember) =>
      JSON.stringify([environmentId, linkGeneration]);
    const targets = new Map<string, WorkstreamFleetMember>(
      fleet.members
        .filter((member) => !member.discovered)
        .map((member) => [peerKey(member), member]),
    );
    for (const operation of operations)
      for (const leg of operation.peers) targets.set(peerKey(leg), leg);
    const checked = new Map(
      yield* Effect.forEach(
        targets,
        ([key, member]) =>
          checkPeer(fleet.authorityId, member).pipe(Effect.map((block) => [key, block] as const)),
        { concurrency: 4 },
      ),
    );
    yield* Effect.forEach(
      fleet.members,
      (member) =>
        Effect.gen(function* () {
          if (member.discovered || checked.get(peerKey(member))) return;
          const response = yield* peers
            .call({
              environmentId: member.environmentId,
              expectedGeneration: member.linkGeneration,
              tool: "t3_workstream_definitions_list",
              arguments: { authorityEnvironmentId: fleet.authorityId },
              success: Protocol.FleetPeerDefinitions,
              limits,
            })
            .pipe(Effect.result);
          if (response._tag === "Success")
            yield* workstreams.recordPeerDiscovery(
              member,
              fleet.revision,
              response.success.workstreams,
            );
        }),
      { concurrency: 4, discard: true },
    );
    yield* Effect.forEach(
      operations,
      (operation) =>
        Effect.gen(function* () {
          const legs = yield* Effect.forEach(
            operation.peers,
            (leg) => readLeg(fleet.authorityId, operation, leg, checked.get(peerKey(leg)) ?? null),
            { concurrency: 4 },
          );
          if (operation.kind === "close" && legs.some((leg) => leg.kind === "blocked")) return;
          // Serialize writes within one close so a known rejection stops later writes.
          for (const leg of legs) {
            if (leg.kind !== "ready") continue;
            const delivered = yield* deliver(fleet.authorityId, operation, leg);
            if (!delivered && operation.kind === "close") return;
          }
          yield* workstreams.finalizeOperation(operation.requestId);
        }),
      { discard: true },
    );
  });
  return WorkstreamFleetWorker.of({ manage, reconcile });
});

const layer = Layer.effect(WorkstreamFleetWorker, make);
export const workerLayer = Layer.effectDiscard(
  Effect.gen(function* () {
    const worker = yield* WorkstreamFleetWorker;
    yield* worker.reconcile.pipe(
      Effect.catch((error) =>
        Effect.logWarning("Workstream fleet reconciliation failed", { code: error.code }),
      ),
      Effect.repeat(Schedule.spaced("5 seconds")),
      Effect.forkScoped,
    );
  }),
).pipe(Layer.provideMerge(layer));
