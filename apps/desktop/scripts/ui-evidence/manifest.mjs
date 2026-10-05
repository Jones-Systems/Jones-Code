import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";

export const NOT_COVERED = [
  "macOS",
  "packaged/fused builds",
  "signing",
  "native dialogs",
  "provider/network flows",
  "message content",
  "GitHub attachment upload",
];
const digest = (value) => typeof value === "string" && /^[a-f0-9]{64}$/.test(value);
const oid = (value) => typeof value === "string" && /^[a-f0-9]{40}$/.test(value);
export function validateManifest(value) {
  if (
    value?.schema !== "jones-code-ui-evidence/v1" ||
    typeof value.runId !== "string" ||
    !value.runId
  )
    throw new Error("Invalid manifest identity");
  if (!["passed", "failed", "cancelled", "rejected"].includes(value.status))
    throw new Error("Invalid manifest status");
  if (!oid(value.source?.head) || !oid(value.harness?.head))
    throw new Error("Manifest needs full source and harness OIDs");
  if (
    !digest(value.source?.statusSha256) ||
    !digest(value.source?.patch?.sha256) ||
    !digest(value.source?.untracked?.sha256)
  )
    throw new Error("Manifest source digests missing");
  for (const name of ["desktop", "server", "web", "boot"])
    if (!digest(value.build?.[name]?.sha256))
      throw new Error(`Manifest build digest missing: ${name}`);
  if (!digest(value.scenario?.sha256)) throw new Error("Manifest scenario digest missing");
  if (
    !Array.isArray(value.captures) ||
    !Array.isArray(value.scenario.steps) ||
    !Array.isArray(value.assertions)
  )
    throw new Error("Manifest scenario results missing");
  const names = new Set();
  for (const capture of value.captures) {
    if (
      !digest(capture.sha256) ||
      !Number.isInteger(capture.width) ||
      capture.width < 1 ||
      !Number.isInteger(capture.height) ||
      capture.height < 1 ||
      !Number.isFinite(capture.scale) ||
      capture.scale <= 0 ||
      !["dark", "light"].includes(capture.theme) ||
      !/^[a-zA-Z0-9][a-zA-Z0-9_-]{0,79}\.png$/.test(capture.file) ||
      names.has(capture.file)
    )
      throw new Error("Invalid manifest capture");
    names.add(capture.file);
  }
  if (!value.cleanup || typeof value.cleanup !== "object")
    throw new Error("Manifest cleanup result missing");
  if (
    value.status === "passed" &&
    (value.scenario.steps.some((step) => step.status !== "passed") ||
      value.assertions.some((assertion) => assertion.status !== "passed"))
  )
    throw new Error("Passing manifest contains failed/incomplete scenario evidence");
  return value;
}
export function createManifest({ runId, provenance, scenario, inner = {}, cleanup, status }) {
  const result = {
    schema: "jones-code-ui-evidence/v1",
    runId,
    recordedAt: new Date().toISOString(),
    ...provenance,
    runtime: inner.runtime ?? null,
    window: inner.window ?? null,
    themeFixture: inner.themeFixture ?? null,
    fixture: inner.fixture ?? {},
    scenario: { ...scenario, steps: inner.steps ?? [] },
    assertions: inner.assertions ?? [],
    captures: inner.captures ?? [],
    isolation: { ...inner.isolation, topology: inner.topology ?? null },
    resources: inner.resources ?? null,
    namespaceProcess: inner.namespaceProcess ?? null,
    coverage: {
      claims:
        inner.assertions?.filter((item) => item.status === "passed").map((item) => item.name) ?? [],
      notCovered: NOT_COVERED,
      comparison:
        "Captures show ordered interactions on one candidate build; a comparison OID alone does not establish a code-change before/after result.",
    },
    status: status ?? inner.status ?? "failed",
    cleanup: cleanup ?? { outcome: "unknown" },
    error: inner.error ?? null,
  };
  return validateManifest(result);
}
export async function writeManifest(artifacts, manifest) {
  validateManifest(manifest);
  await NodeFSP.writeFile(
    NodePath.join(artifacts, "manifest.json"),
    `${JSON.stringify(manifest, null, 2)}\n`,
    {
      mode: 0o600,
      flag: "wx",
    },
  );
  return manifest;
}
