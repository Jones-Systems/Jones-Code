// @effect-diagnostics-next-line nodeBuiltinImport:off -- Quota proof binding uses synchronous HMAC and private random key generation; Effect Crypto has no HMAC API.
import * as NodeCrypto from "node:crypto";
import * as Schema from "effect/Schema";
import type {
  QualifiedQuota,
  QualifiedQuotaMap,
  ServerProviderModel,
  ProviderInstanceId,
} from "@t3tools/contracts";

export const PrivateQuotaProof = Schema.Struct({
  instanceId: Schema.String,
  probeId: Schema.String,
  bindingGeneration: Schema.String,
  probeGeneration: Schema.String,
  identityTag: Schema.NullOr(Schema.String),
  ordinaryUsageAllowed: Schema.Literal(true),
});
export type PrivateQuotaProof = typeof PrivateQuotaProof.Type;
interface Binding {
  key: Uint8Array;
  bindingGeneration: string;
  identityTag: string | null;
  probeGeneration: string;
}
export interface QuotaProbeBinding {
  readonly instanceId: ProviderInstanceId;
  readonly probeId: string;
  readonly bindingGeneration: string;
  readonly probeGeneration: string;
}
const bindings = new Map<ProviderInstanceId, Binding>();
const proofs = new WeakMap<QualifiedQuota, PrivateQuotaProof>();

export function resetQuotaBinding(instanceId: ProviderInstanceId): void {
  bindings.set(instanceId, {
    key: NodeCrypto.randomBytes(32),
    bindingGeneration: NodeCrypto.randomUUID(),
    identityTag: null,
    probeGeneration: NodeCrypto.randomUUID(),
  });
}

export function beginQuotaProbe(
  instanceId: ProviderInstanceId,
  probeId: string,
): QuotaProbeBinding {
  const binding = bindings.get(instanceId) ?? {
    key: NodeCrypto.randomBytes(32),
    bindingGeneration: NodeCrypto.randomUUID(),
    identityTag: null,
    probeGeneration: NodeCrypto.randomUUID(),
  };
  binding.probeGeneration = NodeCrypto.randomUUID();
  bindings.set(instanceId, binding);
  return {
    instanceId,
    probeId,
    bindingGeneration: binding.bindingGeneration,
    probeGeneration: binding.probeGeneration,
  };
}

export function readPrivateQuotaProof(quota: QualifiedQuota): PrivateQuotaProof | undefined {
  const proof = proofs.get(quota);
  const binding = bindings.get(quota.instanceId);
  return proof &&
    binding &&
    proof.instanceId === quota.instanceId &&
    proof.probeId === quota.probeId &&
    proof.bindingGeneration === binding.bindingGeneration &&
    proof.probeGeneration === binding.probeGeneration
    ? proof
    : undefined;
}

export function samePrivateQuotaProof(
  left: PrivateQuotaProof | null,
  right: PrivateQuotaProof | undefined,
): boolean {
  return (
    !!left &&
    !!right &&
    left.instanceId === right.instanceId &&
    left.probeId === right.probeId &&
    left.bindingGeneration === right.bindingGeneration &&
    left.probeGeneration === right.probeGeneration &&
    left.identityTag === right.identityTag &&
    left.ordinaryUsageAllowed === true &&
    right.ordinaryUsageAllowed === true
  );
}

export function makeQualifiedQuota(input: {
  readonly instanceId: ProviderInstanceId;
  readonly probeId: string;
  readonly probeBinding?: QuotaProbeBinding;
  readonly ordinaryUsageAllowed?: boolean | null | undefined;
  readonly accountId?: string | null | undefined;
  readonly attemptedAt: string;
  readonly probeCompletedAt: string;
  readonly quotaReceivedAt?: string | undefined;
  readonly rateLimitsByLimitId?: QualifiedQuotaMap | null | undefined;
  readonly models?: ReadonlyArray<ServerProviderModel>;
  readonly failureCode?: QualifiedQuota["failureCode"];
}): QualifiedQuota {
  const probe = input.probeBinding ?? beginQuotaProbe(input.instanceId, input.probeId);
  const binding = bindings.get(input.instanceId);
  const currentProbe =
    binding &&
    probe.instanceId === input.instanceId &&
    probe.probeId === input.probeId &&
    probe.bindingGeneration === binding.bindingGeneration &&
    probe.probeGeneration === binding.probeGeneration;
  if (currentProbe && input.accountId != null) {
    const identityTag = NodeCrypto.createHmac("sha256", binding.key)
      .update("codex-quota-account/v1\0")
      .update(input.instanceId)
      .update("\0")
      .update(input.accountId)
      .digest("hex");
    if (binding.identityTag !== null && binding.identityTag !== identityTag)
      binding.bindingGeneration = NodeCrypto.randomUUID();
    binding.identityTag = identityTag;
  }
  const map = input.rateLimitsByLimitId ?? null;
  const windowProvenance: QualifiedQuota["windowProvenance"][number][] = [];
  let complete = map !== null && Object.keys(map).length > 0 && input.quotaReceivedAt !== undefined;
  for (const [limitId, bucket] of Object.entries(map ?? {})) {
    if (
      bucket.individualLimit != null ||
      bucket.spendControlReached === true ||
      bucket.rateLimitReachedType != null
    ) {
      complete = false;
    }
    let windows = 0;
    for (const windowId of ["primary", "secondary"] as const) {
      const window = bucket[windowId];
      if (!window) continue;
      windows++;
      if (input.quotaReceivedAt) {
        windowProvenance.push({
          limitId,
          windowId,
          probeId: input.probeId,
          quotaReceivedAt: input.quotaReceivedAt,
        });
      }
      if (
        !Number.isFinite(window.usedPercent) ||
        window.usedPercent < 0 ||
        window.usedPercent > 100 ||
        typeof window.resetsAt !== "number" ||
        !Number.isFinite(window.resetsAt) ||
        typeof window.windowDurationMins !== "number" ||
        !Number.isFinite(window.windowDurationMins) ||
        window.windowDurationMins <= 0
      ) {
        complete = false;
      }
    }
    if (windows === 0) complete = false;
  }
  const failureCode =
    input.failureCode ??
    (!currentProbe
      ? "refresh_not_observed"
      : input.ordinaryUsageAllowed !== true
        ? "quota_unavailable"
        : !map
          ? "missing_map"
          : !complete
            ? "incomplete_windows"
            : null);
  const unsupported = failureCode === "unsupported_account" || failureCode === "disabled";
  const failed =
    failureCode === "probe_failed" ||
    failureCode === "probe_timeout" ||
    failureCode === "quota_unavailable";
  const quota: QualifiedQuota = {
    schemaVersion: "codex.t3-qualified-quota/v1",
    instanceId: input.instanceId,
    probeId: input.probeId,
    status: unsupported ? "unsupported" : failed ? "failed" : failureCode ? "unknown" : "qualified",
    attemptedAt: input.attemptedAt,
    quotaReceivedAt: input.quotaReceivedAt ?? null,
    probeCompletedAt: input.probeCompletedAt,
    complete: complete && failureCode === null,
    rateLimitsByLimitId: map,
    windowProvenance,
    failureCode,
    capabilityRefs: (input.models ?? [])
      .filter((model) => !model.isCustom)
      .map((model) => ({
        modelId: model.slug,
        reasoningEfforts:
          model.capabilities?.optionDescriptors?.flatMap((descriptor) =>
            descriptor.id === "reasoningEffort" && descriptor.type === "select"
              ? descriptor.options.map((option) => option.id)
              : [],
          ) ?? [],
        source: "model/list",
        probeId: input.probeId,
        observedAt: input.probeCompletedAt,
      })),
  };
  if (currentProbe && input.ordinaryUsageAllowed === true && quota.status === "qualified") {
    proofs.set(quota, {
      instanceId: input.instanceId,
      probeId: input.probeId,
      bindingGeneration: binding.bindingGeneration,
      probeGeneration: binding.probeGeneration,
      identityTag: binding.identityTag,
      ordinaryUsageAllowed: true,
    });
  }
  return quota;
}

export function quotaResetCrossed(quota: QualifiedQuota, now: number): boolean {
  if (quota.status !== "qualified") return false;
  return Object.values(quota.rateLimitsByLimitId ?? {}).some((bucket) =>
    [bucket.primary, bucket.secondary, bucket.individualLimit].some(
      (window) => typeof window?.resetsAt === "number" && window.resetsAt * 1000 <= now,
    ),
  );
}
