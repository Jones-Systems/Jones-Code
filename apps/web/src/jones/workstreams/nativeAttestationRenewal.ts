import type {
  MembershipEpisode,
  NativeReference,
  T3PlacementIdentity,
  T3WorkstreamListResult,
  WorkstreamCommand,
  WorkstreamMembershipPage,
  WorkstreamReceipt,
  WorkstreamReferenceDetail,
  WorkstreamReferencePage,
  WorkstreamsRegistrationContextAcceptedResponse,
} from "@t3tools/contracts";
import { EnvironmentHttpConflictError } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import {
  workstreamBindingKey,
  type LiveT3Placements,
} from "@t3tools/client-runtime/state/workstreams";
import { attestedNativeThreadKey } from "../../components/workstreams/nativeThreadGrouping";
import { randomUUID } from "../../lib/utils";
import { canEditWorkstreams } from "../../components/workstreams/nativeWorkstreamActions";
import { assertWorkstreamReadContext, WorkstreamActionError } from "./workstreamActionSnapshot";

const LEAD_MS = 60_000;
const MAX_REFERENCES = 1_000;
const MAX_WORKSTREAMS = 50;
const JOURNAL_KEY = "jones.workstreams.native-renewal.v1";
const inFlight = new Set<string>();
const isConflict = Schema.is(EnvironmentHttpConflictError);

export interface RenewalSnapshot {
  readonly data: T3WorkstreamListResult;
  readonly context: WorkstreamsRegistrationContextAcceptedResponse;
  readonly placements: LiveT3Placements;
}
interface PendingRenewal {
  readonly authority: string;
  readonly referenceId: string;
  readonly version: number;
  readonly commandId: string;
  readonly terminal: boolean;
}
export interface RenewalStorage {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
}
export function nativeRenewalStorage(): RenewalStorage | null {
  try {
    return typeof localStorage === "undefined" ? null : localStorage;
  } catch {
    return null;
  }
}
interface Candidate {
  readonly referenceId: string;
  readonly environmentId: string;
  readonly threadId: string;
  readonly membershipId: string;
  readonly workstreamId: string;
  readonly kind: "primary" | "secondary";
  readonly version: number;
  readonly expiresAt: number;
}
export interface NativeAttestationRenewalInput {
  readonly initial: RenewalSnapshot;
  readonly identities: readonly T3PlacementIdentity[];
  readonly signal: AbortSignal;
  readonly storage: RenewalStorage;
  readonly load: () => Promise<RenewalSnapshot>;
  readonly references: (cursor?: string) => Promise<WorkstreamReferencePage>;
  readonly reference: (id: string) => Promise<WorkstreamReferenceDetail>;
  readonly memberships: (
    workstreamId: string,
    cursor?: string,
  ) => Promise<WorkstreamMembershipPage>;
  readonly submit: (command: WorkstreamCommand) => Promise<WorkstreamReceipt>;
  readonly observe: (id: string) => Promise<WorkstreamReceipt>;
  readonly publish: (snapshot: RenewalSnapshot) => void;
  readonly unavailable: () => void;
  readonly commandPending?: (id: string, pending: boolean) => void;
  readonly now?: () => number;
  readonly commandId?: () => string;
}

function journal(storage: RenewalStorage): PendingRenewal[] {
  const raw = storage.getItem(JOURNAL_KEY);
  if (raw === null) return [];
  if (raw.length > 1_048_576) throw new WorkstreamActionError("unknown");
  const value: unknown = JSON.parse(raw);
  if (!Array.isArray(value) || value.length > MAX_REFERENCES)
    throw new WorkstreamActionError("unknown");
  return value.map((entry: unknown) => {
    if (
      typeof entry !== "object" ||
      entry === null ||
      !("authority" in entry) ||
      typeof entry.authority !== "string" ||
      entry.authority.length > 16_384 ||
      !("referenceId" in entry) ||
      typeof entry.referenceId !== "string" ||
      entry.referenceId.length > 1_024 ||
      !("commandId" in entry) ||
      typeof entry.commandId !== "string" ||
      entry.commandId.length > 1_024 ||
      !("version" in entry) ||
      typeof entry.version !== "number" ||
      !Number.isSafeInteger(entry.version) ||
      entry.version < 1 ||
      !("terminal" in entry) ||
      typeof entry.terminal !== "boolean"
    )
      throw new WorkstreamActionError("unknown");
    return {
      authority: entry.authority,
      referenceId: entry.referenceId,
      commandId: entry.commandId,
      version: entry.version,
      terminal: entry.terminal,
    };
  });
}
function save(storage: RenewalStorage, record: PendingRenewal) {
  if (
    record.authority.length > 16_384 ||
    record.referenceId.length > 1_024 ||
    record.commandId.length > 1_024
  )
    throw new WorkstreamActionError("unknown");
  const records = journal(storage).filter(
    (entry) => entry.authority !== record.authority || entry.referenceId !== record.referenceId,
  );
  records.push(record);
  if (records.length > MAX_REFERENCES) throw new WorkstreamActionError("unknown");
  const encoded = JSON.stringify(records);
  if (encoded.length > 1_048_576) throw new WorkstreamActionError("unknown");
  storage.setItem(JOURNAL_KEY, encoded);
  if (storage.getItem(JOURNAL_KEY) !== encoded) throw new WorkstreamActionError("unknown");
}

const authorityKey = (snapshot: RenewalSnapshot) =>
  JSON.stringify([
    workstreamBindingKey({ ...snapshot.data.binding, registryVersion: 0 }),
    [...snapshot.data.binding.permissions].sort(),
    snapshot.context.grant_id,
    snapshot.context.sources,
  ]);

function validate(snapshot: RenewalSnapshot, authority: string): void {
  if (
    !canEditWorkstreams(snapshot.data) ||
    authorityKey(snapshot) !== authority ||
    snapshot.context.state !== "ready" ||
    snapshot.placements.readiness !== "ready"
  )
    throw new WorkstreamActionError("stale");
  for (const context of [snapshot.context, snapshot.placements.context]) {
    assertWorkstreamReadContext(snapshot.data, context);
    if (
      context.principal_id !== snapshot.data.binding.principalId ||
      context.authorization_revision !== snapshot.data.binding.authorizationRevision
    )
      throw new WorkstreamActionError("stale");
  }
}
function localSource(snapshot: RenewalSnapshot, environmentId: string) {
  const sources = snapshot.context.sources.filter(
    (source) => source.provider === "t3" && source.source_instance_id === environmentId,
  );
  const trust = snapshot.placements.trustedEnvironments.find(
    (entry) => entry.environmentId === environmentId,
  );
  return (
    sources.length === 1 &&
    trust &&
    sources[0]?.authority_namespace === trust.authorityNamespace &&
    sources[0]?.store_generation === trust.storeGeneration
  );
}
function matches(
  reference: NativeReference,
  candidate: Pick<Candidate, "referenceId" | "environmentId" | "threadId">,
  ownerId: string,
) {
  const identity = reference.identity;
  return (
    reference.native_reference_id === candidate.referenceId &&
    reference.owner_id === ownerId &&
    reference.pr_locator === null &&
    identity.provider === "t3" &&
    identity.source_instance_id === candidate.environmentId &&
    identity.native_id === candidate.threadId &&
    identity.resource_kind === "thread" &&
    identity.id_kind === "internal" &&
    identity.account_provenance.kind === "not_account_scoped"
  );
}
function placement(
  snapshot: RenewalSnapshot,
  candidate: Candidate,
  reference: NativeReference,
  now: number,
) {
  const key = attestedNativeThreadKey(
    reference,
    now,
    new Map(snapshot.placements.trustedEnvironments.map((entry) => [entry.environmentId, entry])),
  );
  if (key !== JSON.stringify([candidate.environmentId, candidate.threadId])) return undefined;
  return snapshot.placements.items.find(
    (item) =>
      item.native_reference_id === candidate.referenceId &&
      item.membership_id === candidate.membershipId &&
      item.workstream_id === candidate.workstreamId &&
      item.kind === candidate.kind &&
      item.source_instance_id === candidate.environmentId &&
      item.native_thread_id === candidate.threadId &&
      item.attestation_version === reference.registration.attestation_version &&
      item.expires_at === reference.registration.expires_at &&
      item.attested_at === reference.registration.attested_at &&
      item.evidence_sha256 === reference.registration.evidence?.evidence_sha256 &&
      Date.parse(item.expires_at) > now,
  );
}
function wait(delay: number, signal: AbortSignal): Promise<void> {
  signal.throwIfAborted();
  return new Promise((resolve, reject) => {
    const abort = () => {
      clearTimeout(timer);
      reject(signal.reason);
    };
    const timer = setTimeout(
      () => {
        signal.removeEventListener("abort", abort);
        resolve();
      },
      Math.max(0, Math.min(2_147_483_647, delay)),
    );
    signal.addEventListener("abort", abort, { once: true });
  });
}

/** Metadata finds expired candidates; only a fresh, validated placement can restore routing. */
export async function runNativeAttestationRenewal(
  input: NativeAttestationRenewalInput,
): Promise<void> {
  const now = input.now ?? Date.now;
  const authority = authorityKey(input.initial);
  validate(input.initial, authority);
  for (const record of journal(input.storage))
    if (record.authority === authority && !record.terminal)
      input.commandPending?.(record.commandId, true);
  const check = () => input.signal.throwIfAborted();
  const candidates = new Map<string, Candidate>();
  const identities = new Set(
    input.identities.map((item) =>
      JSON.stringify([item.source_instance_id, item.native_thread_id]),
    ),
  );
  for (const item of input.initial.placements.items) {
    if (
      !localSource(input.initial, item.source_instance_id) ||
      !identities.has(JSON.stringify([item.source_instance_id, item.native_thread_id]))
    )
      continue;
    candidates.set(item.native_reference_id, {
      referenceId: item.native_reference_id,
      environmentId: item.source_instance_id,
      threadId: item.native_thread_id,
      membershipId: item.membership_id,
      workstreamId: item.workstream_id,
      kind: item.kind,
      version: item.attestation_version,
      expiresAt: Date.parse(item.expires_at),
    });
  }
  const memberships = async (
    snapshot: RenewalSnapshot,
    workstreamId: string,
    budget?: { pages: number },
  ): Promise<readonly MembershipEpisode[]> => {
    let cursor: string | undefined;
    const items: MembershipEpisode[] = [];
    const cursors = new Set<string>();
    for (;;) {
      check();
      if (budget && budget.pages-- === 0) throw new WorkstreamActionError("stale");
      const page = await input.memberships(workstreamId, cursor);
      check();
      assertWorkstreamReadContext(snapshot.data, page.context);
      if (page.items.length > 100 || page.items.some((item) => item.workstream_id !== workstreamId))
        throw new WorkstreamActionError("stale");
      items.push(...page.items);
      if (items.length > MAX_REFERENCES) throw new WorkstreamActionError("stale");
      if (page.next_cursor === null) return items;
      if (cursors.has(page.next_cursor) || cursors.size >= 9)
        throw new WorkstreamActionError("stale");
      cursors.add(page.next_cursor);
      cursor = page.next_cursor;
    }
  };
  // One bounded discovery pass per load/reconnect, never a scan of native stores.
  if (
    candidates.size < input.identities.length &&
    input.initial.context.sources.some((source) => source.provider === "t3")
  ) {
    const references: NativeReference[] = [];
    let cursor: string | undefined;
    const cursors = new Set<string>();
    for (let pageIndex = 0; pageIndex < 10; pageIndex++) {
      check();
      const page = await input.references(cursor);
      check();
      assertWorkstreamReadContext(input.initial.data, page.context);
      references.push(...page.items);
      if (references.length > MAX_REFERENCES) throw new WorkstreamActionError("stale");
      if (page.next_cursor === null) break;
      if (cursors.has(page.next_cursor)) throw new WorkstreamActionError("stale");
      cursors.add(page.next_cursor);
      cursor = page.next_cursor;
      if (pageIndex === 9) input.unavailable();
    }
    const eligible = references.filter((reference) => {
      const identity = reference.identity;
      return (
        !candidates.has(reference.native_reference_id) &&
        reference.registration.attestation_version > 0 &&
        identities.has(JSON.stringify([identity.source_instance_id, identity.native_id])) &&
        localSource(input.initial, identity.source_instance_id) &&
        matches(
          reference,
          {
            referenceId: reference.native_reference_id,
            environmentId: identity.source_instance_id,
            threadId: identity.native_id,
          },
          input.initial.data.binding.ownerId,
        )
      );
    });
    if (eligible.length) {
      const budget = { pages: 10 };
      if (input.initial.data.items.length > MAX_WORKSTREAMS) input.unavailable();
      for (const workstream of input.initial.data.items.slice(0, MAX_WORKSTREAMS)) {
        if (budget.pages === 0) {
          input.unavailable();
          break;
        }
        const open = await memberships(input.initial, workstream.workstreamId, budget);
        for (const member of open) {
          const reference = eligible.find(
            (entry) => entry.native_reference_id === member.native_reference_id,
          );
          if (member.closed !== null || !reference || candidates.has(reference.native_reference_id))
            continue;
          candidates.set(reference.native_reference_id, {
            referenceId: reference.native_reference_id,
            environmentId: reference.identity.source_instance_id,
            threadId: reference.identity.native_id,
            membershipId: member.membership_id,
            workstreamId: member.workstream_id,
            kind: member.kind,
            version: reference.registration.attestation_version,
            expiresAt:
              reference.registration.expires_at === null
                ? 0
                : Date.parse(reference.registration.expires_at),
          });
        }
      }
    }
  }
  const findRecord = (candidate: Candidate) =>
    journal(input.storage).find(
      (entry) => entry.authority === authority && entry.referenceId === candidate.referenceId,
    );
  while (candidates.size) {
    check();
    const due = [...candidates.values()].sort((a, b) => a.expiresAt - b.expiresAt)[0]!;
    const record = findRecord(due);
    if (record?.terminal && record.version >= due.version) {
      candidates.delete(due.referenceId);
      continue;
    }
    if (!record || record.terminal) await wait(due.expiresAt - LEAD_MS - now(), input.signal);
    check();
    const flightKey = JSON.stringify([authority, due.referenceId]);
    if (inFlight.has(flightKey)) {
      candidates.delete(due.referenceId);
      continue;
    }
    inFlight.add(flightKey);
    try {
      const load = async () => {
        check();
        const snapshot = await input.load();
        check();
        validate(snapshot, authority);
        if (!localSource(snapshot, due.environmentId))
          throw new WorkstreamActionError("activation");
        const detail = await input.reference(due.referenceId);
        check();
        assertWorkstreamReadContext(snapshot.data, detail.context);
        if (
          !matches(detail.reference, due, snapshot.data.binding.ownerId) ||
          detail.reference.registration.attestation_version < due.version
        )
          throw new WorkstreamActionError("stale");
        const open = await memberships(snapshot, due.workstreamId);
        if (
          !open.some(
            (member) =>
              member.membership_id === due.membershipId &&
              member.native_reference_id === due.referenceId &&
              member.kind === due.kind &&
              member.closed === null,
          )
        )
          throw new WorkstreamActionError("stale");
        return { snapshot, reference: detail.reference };
      };
      let fresh = await load();
      const pending = findRecord(due);
      let observedVersion = fresh.reference.registration.attestation_version;
      let receipt: WorkstreamReceipt;
      let commandRecord: PendingRenewal;
      if (pending && !pending.terminal) {
        commandRecord = pending;
        observedVersion = pending.version;
        receipt = await input.observe(pending.commandId);
      } else {
        const current = placement(fresh.snapshot, due, fresh.reference, now());
        if (
          current &&
          (observedVersion > due.version || Date.parse(current.expires_at) - LEAD_MS > now())
        ) {
          input.publish(fresh.snapshot);
          candidates.set(due.referenceId, {
            ...due,
            version: observedVersion,
            expiresAt: Date.parse(current.expires_at),
          });
          continue;
        }
        if (pending && pending.version >= observedVersion)
          throw new WorkstreamActionError("unknown");
        check();
        commandRecord = {
          authority,
          referenceId: due.referenceId,
          version: observedVersion,
          commandId: (input.commandId ?? randomUUID)(),
          terminal: false,
        };
        save(input.storage, commandRecord);
        input.commandPending?.(commandRecord.commandId, true);
        try {
          receipt = await input.submit({
            command_id: commandRecord.commandId,
            expected_server_generation: fresh.snapshot.data.binding.serverGeneration,
            expected_registry_version: fresh.snapshot.data.binding.registryVersion,
            action: {
              operation: "verify_reference",
              native_reference_id: due.referenceId,
              expected_attestation_version: observedVersion,
            },
          });
        } catch (cause) {
          check();
          if (isConflict(cause)) {
            const readback = await load();
            if (
              readback.reference.registration.attestation_version > observedVersion &&
              placement(readback.snapshot, due, readback.reference, now())
            )
              input.publish(readback.snapshot);
          }
          // Even a conflict does not authorize a new command ID or replay.
          throw cause;
        }
      }
      for (let observation = 0; ; observation++) {
        check();
        if (
          receipt.command_id !== commandRecord.commandId ||
          receipt.owner_id !== fresh.snapshot.data.binding.ownerId ||
          receipt.actor.principal_id !== fresh.snapshot.data.binding.principalId ||
          receipt.server_generation !== fresh.snapshot.data.binding.serverGeneration ||
          receipt.operation !== "verify_reference"
        )
          throw new WorkstreamActionError("unknown");
        if (
          receipt.state === "committed" &&
          receipt.effects.native_reference_id !== due.referenceId
        )
          throw new WorkstreamActionError("unknown");
        if (receipt.state !== "pending" && receipt.state !== "unresolved") break;
        if (observation === 4) throw new WorkstreamActionError("unknown");
        await wait(
          Math.min(30_000, Math.max(250, receipt.retry_after_seconds * 1_000)),
          input.signal,
        );
        check();
        receipt = await input.observe(commandRecord.commandId);
      }
      save(input.storage, { ...commandRecord, terminal: true });
      input.commandPending?.(commandRecord.commandId, false);
      // A committed verification can still report failure. Read back proof and unchanged membership.
      fresh = await load();
      const current = placement(fresh.snapshot, due, fresh.reference, now());
      if (
        !current ||
        fresh.reference.registration.attestation_version <= observedVersion ||
        (receipt.state === "committed" &&
          fresh.snapshot.data.binding.registryVersion < receipt.registry_version)
      )
        throw new WorkstreamActionError("stale");
      input.publish(fresh.snapshot);
      candidates.set(due.referenceId, {
        ...due,
        version: fresh.reference.registration.attestation_version,
        expiresAt: Date.parse(current.expires_at),
      });
    } catch {
      check();
      candidates.delete(due.referenceId);
      input.unavailable();
    } finally {
      inFlight.delete(flightKey);
    }
  }
}
