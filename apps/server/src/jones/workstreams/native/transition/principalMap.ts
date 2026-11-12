import { canonical, refuse } from "./canonical.ts";
export interface PrincipalMapping {
  readonly principal_id: string;
  readonly environment_id: string;
  readonly source_instance_id: string;
  readonly evidence_sha256: string;
}
export function validatePrincipalMap(
  mappings: readonly PrincipalMapping[],
  principals: ReadonlySet<string>,
  sources: ReadonlySet<string>,
): void {
  const seen = new Set<string>(),
    environments = new Set<string>();
  for (const m of mappings) {
    if (
      !principals.has(m.principal_id) ||
      !sources.has(m.source_instance_id) ||
      m.environment_id !== m.source_instance_id ||
      !/^[a-f0-9]{64}$/.test(m.evidence_sha256) ||
      seen.has(m.principal_id) ||
      environments.has(m.environment_id)
    )
      refuse("mapping_conflict");
    seen.add(m.principal_id);
    environments.add(m.environment_id);
  }
}
export function mappingDigest(mappings: readonly PrincipalMapping[]): string {
  return canonical(
    [...mappings].sort((a, b) =>
      a.principal_id < b.principal_id ? -1 : a.principal_id > b.principal_id ? 1 : 0,
    ),
  );
}
