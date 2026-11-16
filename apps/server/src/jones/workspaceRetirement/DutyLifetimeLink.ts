import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { digest, type RuntimeUseBinding } from "../cleanup/RuntimeUseProtocol.ts";
import { makeRuntimeUseStore } from "../cleanup/RuntimeUseStore.ts";
import { NativeDutyOwnerJson } from "./DurableCodecs.ts";
import { unresolvedOwner, type NativeDutyOwner } from "./NativeDutyOwner.ts";
import type { PersistedDuty } from "./ClaimReconciliation.ts";

export interface DutyClaimHandle {
  readonly id: string;
  readonly holder: string;
  readonly owner: string;
}

// Present only around the exact open that acquired this claim.
export class OpenDutyClaim extends Context.Service<
  OpenDutyClaim,
  { readonly link: (binding: RuntimeUseBinding) => Effect.Effect<void> }
>()("t3/jones/workspaceRetirement/DutyLifetimeLink/OpenDutyClaim") {}

const decodeOwner = Schema.decodeEffect(NativeDutyOwnerJson);
const encodeOwner = Schema.encodeEffect(NativeDutyOwnerJson);

export const bindOwner = Effect.fn("WorkspaceDuty.bindOwner")(function* (
  claim: DutyClaimHandle,
  incoming: NativeDutyOwner,
) {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql.withTransaction(
    Effect.gen(function* () {
      const rows = yield* sql<{ native_owner_json: string | null }>`SELECT i.native_owner_json
      FROM jones_workspace_duty_identity i JOIN jones_workspace_duty_claims c ON c.claim_id=i.claim_id
      WHERE c.claim_id=${claim.id} AND c.holder_json=${claim.holder} AND c.released_at IS NULL`;
      const row = rows[0];
      if (!row || row.native_owner_json === null) return;
      const prior = yield* decodeOwner(row.native_owner_json);
      // A downgrade is permanent. Failed capture cannot replace observed coverage.
      if (prior.type === "unresolved" && prior.reason === "multiple_producer_generations")
        return prior;
      let owner =
        incoming.type === "unresolved" && incoming.host === undefined && prior.host !== undefined
          ? { ...incoming, host: prior.host }
          : incoming;
      if (prior.type !== "unresolved") {
        if (incoming.type === "unresolved" && incoming.reason !== "multiple_producer_generations")
          return;
        if (digest(prior) !== digest(incoming))
          owner = unresolvedOwner("multiple_producer_generations", prior.host);
      }
      yield* sql`UPDATE jones_workspace_duty_identity SET native_owner_json=${yield* encodeOwner(owner)}
      WHERE claim_id=${claim.id} AND native_owner_json=${row.native_owner_json}
      AND EXISTS (SELECT 1 FROM jones_workspace_duty_claims c
        WHERE c.claim_id=${claim.id} AND c.holder_json=${claim.holder} AND c.released_at IS NULL)`;
      return owner;
    }),
  );
});

export const linkLifetime = Effect.fn("WorkspaceDuty.linkLifetime")(function* (
  claim: DutyClaimHandle,
  binding: RuntimeUseBinding,
) {
  const sql = yield* SqlClient.SqlClient;
  if (binding.actor.kind !== "provider_open" || binding.actor.provider.sessionId !== claim.owner)
    return yield* Effect.die("workspace_duty_open_owner_mismatch");
  yield* sql`INSERT INTO jones_workspace_duty_lifetimes
    (claim_id,holder_sha256,use_id,binding_sha256,linked_at)
    SELECT ${claim.id},${digest(claim.holder)},${binding.useId},${digest(binding)},${DateTime.formatIso(DateTime.nowUnsafe())}
    WHERE EXISTS (SELECT 1 FROM jones_workspace_duty_claims
      WHERE claim_id=${claim.id} AND holder_json=${claim.holder} AND kind='session'
        AND owner=${claim.owner} AND released_at IS NULL)
    ON CONFLICT(claim_id) DO NOTHING`;
});

// null means unlinked; false means linked but unproved. Never fall back to PID
// absence for linked claims, including malformed or unavailable journal evidence.
export const retiredLifetime = Effect.fn("WorkspaceDuty.retiredLifetime")(
  function* (claim: PersistedDuty) {
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql<{
      holder_sha256: string;
      use_id: string;
      binding_sha256: string;
    }>`SELECT
    holder_sha256,use_id,binding_sha256 FROM jones_workspace_duty_lifetimes WHERE claim_id=${claim.claim_id}`;
    const link = rows[0];
    if (!link) return null;
    if (link.holder_sha256 !== digest(claim.holder_json)) return false;
    const store = yield* makeRuntimeUseStore;
    const record = yield* store.read(link.use_id);
    if (
      !record ||
      digest(record.binding) !== link.binding_sha256 ||
      record.state !== "retired" ||
      record.binding.actor.kind !== "provider_open" ||
      record.binding.actor.provider.sessionId !== claim.owner
    )
      return false;
    const proof = record.retirement;
    return (
      proof?.outcome === "not_invoked" ||
      (proof?.outcome === "producers_exited" &&
        proof.producers.length > 0 &&
        proof.producers.every((p) => p.providerSessionId === claim.owner))
    );
  },
  Effect.catchCause(() => Effect.succeed(false)),
);
