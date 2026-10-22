// Independently pinned from Jones-Systems/chatgpt-control-plane at 562bd5cb51de8410560b63bba9119a104be3bb34.

const REGISTRY_BINDING_PROTOCOL = "workstreams-registry-binding/1.0.0" as const;
export const REGISTRY_BINDING_ALGORITHM = "hmac-sha256-workstreams-registry-v1";
export const REGISTRY_BINDING_MANIFEST_SHA256 =
  "15ce45061966ce9c247192d6ef43724b6638bb0e7ccf7bbe9eae4afdd576f30c";
const CANONICAL_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const REGISTRY_ID = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
export interface RegistryIdentity {
  readonly registry_instance_id: string;
  readonly canonical_origin: string;
}
export interface RegistryBinding extends RegistryIdentity {
  readonly protocol: typeof REGISTRY_BINDING_PROTOCOL;
  readonly manifest_sha256: string;
  readonly owner_id: string;
  readonly principal_id: string;
  readonly authorization_revision: number;
}
export type RegistryDispatch = "not-dispatched" | "dispatched";
function canonicalRegistryOrigin(value: unknown): value is string {
  if (typeof value !== "string" || value.length > 512) return false;
  try {
    const url = new URL(value);
    return url.protocol === "https:" && !url.username && !url.password && url.origin === value;
  } catch {
    return false;
  }
}
function encodeRegistryBinding(value: RegistryBinding): string {
  if (
    value.protocol !== REGISTRY_BINDING_PROTOCOL ||
    value.manifest_sha256 !== REGISTRY_BINDING_MANIFEST_SHA256 ||
    typeof value.registry_instance_id !== "string" ||
    !CANONICAL_UUID.test(value.registry_instance_id) ||
    !canonicalRegistryOrigin(value.canonical_origin) ||
    typeof value.owner_id !== "string" ||
    !REGISTRY_ID.test(value.owner_id) ||
    typeof value.principal_id !== "string" ||
    !CANONICAL_UUID.test(value.principal_id) ||
    !Number.isSafeInteger(value.authorization_revision) ||
    value.authorization_revision < 1 ||
    Object.keys(value).sort().join(",") !==
      "authorization_revision,canonical_origin,manifest_sha256,owner_id,principal_id,protocol,registry_instance_id"
  ) {
    throw new Error("invalid_registry_binding");
  }
  return Buffer.from(
    JSON.stringify({
      protocol: value.protocol,
      manifest_sha256: value.manifest_sha256,
      registry_instance_id: value.registry_instance_id,
      canonical_origin: value.canonical_origin,
      owner_id: value.owner_id,
      principal_id: value.principal_id,
      authorization_revision: value.authorization_revision,
    }),
  ).toString("base64url");
}
export function decodeRegistryBinding(token: string): RegistryBinding {
  if (typeof token !== "string" || token.length > 2731 || !/^[A-Za-z0-9_-]+$/.test(token))
    throw new Error("invalid_registry_binding");
  const bytes = Buffer.from(token, "base64url");
  if (bytes.length > 2048) throw new Error("invalid_registry_binding");
  const value = JSON.parse(
    new TextDecoder("utf-8", { fatal: true }).decode(bytes),
  ) as RegistryBinding;
  if (!value || encodeRegistryBinding(value) !== token) throw new Error("invalid_registry_binding");
  return value;
}
