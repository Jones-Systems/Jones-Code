import type { T3WorkstreamBinding, T3WorkstreamListResult } from "@t3tools/contracts";
import {
  WORKSTREAMS_NATIVE_CONTRACT,
  type WorkstreamsNativeCapability,
} from "@t3tools/contracts/jones/workstreamsNative";
import { WorkstreamsNativeReadEnvelope } from "@t3tools/contracts/jones/workstreamsNativeRead";
import * as Schema from "effect/Schema";

const decodeNativeRead = Schema.decodeUnknownSync(WorkstreamsNativeReadEnvelope);

export type NativeWorkstreamsCapability = WorkstreamsNativeCapability;
export const nativeWorkstreamsCapability = (
  capabilities: { readonly workstreamsNative?: WorkstreamsNativeCapability } | null | undefined,
): WorkstreamsNativeCapability | null =>
  capabilities?.workstreamsNative?.contract === WORKSTREAMS_NATIVE_CONTRACT
    ? capabilities.workstreamsNative
    : null;

export const isNativeWorkstreams = (capability: WorkstreamsNativeCapability | null | undefined) =>
  capability?.mode === "native-home" || capability?.mode === "native-member";

export function nativeWorkstreamsBlock(
  capability: WorkstreamsNativeCapability | null | undefined,
): string | null {
  if (capability?.mode === "transition-fenced")
    return "Workstreams are fenced during transition. Resolve the authority fence on this server, then refresh.";
  if (capability?.mode === "disabled")
    return "Native Workstreams are disabled. Configure native Workstreams on this server, then refresh.";
  if (capability?.mode === "native-member" && capability.home_link !== "ok")
    return `Workstream home ${capability.registry.home_environment_id} is unavailable (home link ${capability.home_link}). Restore its link, then refresh. Cached Workstreams are read-only.`;
  return null;
}

export function nativeWorkstreamsMessage(cause: unknown): string | null {
  const value = cause && typeof cause === "object" ? (cause as Record<string, unknown>) : {};
  const code =
    typeof value.code === "string"
      ? value.code
      : typeof value.message === "string"
        ? value.message
        : "";
  switch (code) {
    case "home_route_required":
      return `Open Workstreams on home ${typeof value.home_environment_id === "string" ? value.home_environment_id : "environment"} to make this change.`;
    case "key_unavailable":
      return "The Workstream encryption key is unavailable. Restore the configured key on the owning server, then refresh. No key is created automatically.";
    case "fenced":
    case "authority_fenced":
      return "Workstreams are fenced. Resolve the authority fence on the owning server, then refresh.";
    case "home_unreachable":
    case "offline":
      return "The Workstream home is offline. Restore its connection, then refresh. Cached Workstreams are read-only.";
    case "generation_mismatch":
    case "generation_conflict":
      return "The Workstream home generation changed. Refresh the registry binding; pending changes must reconcile against their original home.";
    case "import_incomplete":
      return "The Workstream import is incomplete. Finish verification and activation on the home, then refresh.";
    default:
      return null;
  }
}

/** Native identity uses registry, owner and home generation. */
export function nativeWorkstreamGroupKey(capability: WorkstreamsNativeCapability): string | null {
  if (!isNativeWorkstreams(capability) || !capability.registry) return null;
  const registry = capability.registry;
  return JSON.stringify([
    registry.registry_instance_id,
    registry.owner_id,
    registry.server_generation,
  ]);
}

export function assertNativeWorkstreamBinding(
  capability: WorkstreamsNativeCapability | null | undefined,
  binding: T3WorkstreamBinding,
): void {
  if (!isNativeWorkstreams(capability) || !capability?.registry) return;
  const registry = capability.registry;
  if (
    binding.registryId !== registry.canonical_origin ||
    binding.ownerId !== registry.owner_id ||
    binding.serverGeneration !== registry.server_generation
  )
    throw new Error(
      "Workstream native registry identity or generation changed. Refresh before using this snapshot.",
    );
}

export function validateNativeWorkstreamRead(
  input: unknown,
  capability: WorkstreamsNativeCapability,
  data: T3WorkstreamListResult,
): WorkstreamsNativeReadEnvelope {
  const read = decodeNativeRead(input);
  assertNativeWorkstreamBinding(capability, data.binding);
  const registry = capability.registry;
  if (
    !isNativeWorkstreams(capability) ||
    !registry ||
    read.registry.registry_instance_id !== registry.registry_instance_id ||
    read.registry.owner_id !== registry.owner_id ||
    read.registry.canonical_origin !== registry.canonical_origin ||
    read.registry.server_generation !== registry.server_generation ||
    read.registry.registry_version !== data.binding.registryVersion ||
    read.provenance.home_environment_id !== registry.home_environment_id ||
    read.provenance.source !== data.source ||
    read.provenance.stale !== data.stale ||
    (read.coverage.registry === "complete" && data.nextCursor !== null)
  )
    throw new Error(
      "Workstream native read provenance does not match the complete registry snapshot.",
    );
  return read;
}

export function nativeWorkstreamsLabel(
  capability: WorkstreamsNativeCapability | null | undefined,
): string | null {
  if (!capability || capability.mode === "legacy") return null;
  const blocked = nativeWorkstreamsBlock(capability);
  if (blocked) return blocked;
  return capability.registry
    ? `Native ${capability.mode === "native-home" ? "home" : "member"} · Home ${capability.registry.home_environment_id} · Generation ${capability.registry.server_generation}`
    : null;
}

const copyList = (data: T3WorkstreamListResult): T3WorkstreamListResult => ({
  ...data,
  binding: { ...data.binding, permissions: [...data.binding.permissions] },
  items: data.items.map((item) => ({ ...item, progress: { ...item.progress } })),
});
const copyRead = (read: WorkstreamsNativeReadEnvelope): WorkstreamsNativeReadEnvelope => ({
  ...read,
  registry: { ...read.registry },
  provenance: { ...read.provenance },
  coverage: { ...read.coverage, sources: read.coverage.sources.map((source) => ({ ...source })) },
});

/** Protected projection bytes stay in memory and are partitioned by authorization. */
export class NativeWorkstreamReadCache {
  #protectedScope: string | null = null;
  #protected = new Map<string, unknown>();
  writeProtected(scope: string, key: string, value: unknown): void {
    if (this.#protectedScope !== scope) this.purge();
    this.#protectedScope = scope;
    if (this.#protected.size >= 16 && !this.#protected.has(key))
      this.#protected.delete(this.#protected.keys().next().value!);
    this.#protected.set(key, JSON.parse(JSON.stringify(value)));
  }
  readProtected(scope: string, key: string): unknown {
    if (this.#protectedScope !== scope) {
      this.purge();
      return null;
    }
    const value = this.#protected.get(key);
    return value === undefined ? null : JSON.parse(JSON.stringify(value));
  }
  #entry: { data: T3WorkstreamListResult; read: WorkstreamsNativeReadEnvelope } | null = null;
  write(
    capability: WorkstreamsNativeCapability,
    data: T3WorkstreamListResult,
    envelope: unknown,
  ): void {
    const read = validateNativeWorkstreamRead(envelope, capability, data);
    if (
      data.nextCursor !== null ||
      data.stale ||
      data.source !== "live" ||
      read.coverage.registry !== "complete"
    )
      throw new Error("Only complete coherent live native snapshots may enter the cache.");
    this.#entry = { data: copyList(data), read: copyRead(read) };
  }
  read(
    capability: WorkstreamsNativeCapability,
    binding: T3WorkstreamBinding,
  ): { data: T3WorkstreamListResult; read: WorkstreamsNativeReadEnvelope } | null {
    const entry = this.#entry;
    if (
      !entry ||
      !isNativeWorkstreams(capability) ||
      nativeWorkstreamGroupKey(capability) !==
        JSON.stringify([
          entry.read.registry.registry_instance_id,
          entry.read.registry.owner_id,
          entry.read.registry.server_generation,
        ]) ||
      entry.read.provenance.home_environment_id !== capability.registry?.home_environment_id ||
      entry.read.registry.canonical_origin !== capability.registry?.canonical_origin ||
      entry.data.binding.registryId !== binding.registryId ||
      entry.data.binding.ownerId !== binding.ownerId ||
      entry.data.binding.serverGeneration !== binding.serverGeneration ||
      JSON.stringify(entry.data.binding.permissions) !== JSON.stringify(binding.permissions) ||
      entry.data.binding.principalId !== binding.principalId ||
      entry.data.binding.authorizationRevision !== binding.authorizationRevision ||
      entry.data.binding.registryVersion !== binding.registryVersion
    ) {
      this.purge();
      return null;
    }
    assertNativeWorkstreamBinding(capability, binding);
    const data = copyList(entry.data);
    const read = copyRead(entry.read);
    return {
      data: { ...data, source: "cache", stale: true },
      read: {
        ...read,
        provenance: { ...read.provenance, source: "cache", stale: true },
        coverage: { ...read.coverage, registry: "stale" },
      },
    };
  }
  purge(): void {
    this.#entry = null;
    this.#protectedScope = null;
    this.#protected.clear();
  }
}

export function nativeWorkstreamReadLabel(
  read: WorkstreamsNativeReadEnvelope | null | undefined,
): string {
  if (!read) return "Native read provenance unavailable.";
  return `${read.provenance.source === "cache" ? "Cached" : "Live"} registry · ${read.coverage.registry} coverage${read.provenance.stale ? " · Stale · Read-only" : ""}`;
}
