// @effect-diagnostics nodeBuiltinImport:off -- Fixed synthetic qualification identity, not provider data.
import * as NodeCrypto from "node:crypto";
import type { WorkspaceIndexVariant } from "./computeContract.ts";

export const qualificationFixture = Object.freeze({
  schema: "jones.server-parallelism.fixture/v2",
  repetitions: 4096,
  residentOrder: "logical-alternation/variant-blocks-by-N",
  zeroMatchQuery: "alpha(?: beta)? alpha",
  zeroMatchUseRegex: true,
  maxFileBytes: 128 * 1024,
  maxWorkspaceBytes: 64 * 1024 * 1024,
});
export const qualificationFixtureId = NodeCrypto.createHash("sha256")
  .update(JSON.stringify(qualificationFixture))
  .digest("hex");

export function qualificationContent(key: number, files = 512): string {
  if (!Number.isSafeInteger(key) || key < 0 || key >= 24) throw new Error("Invalid fixture key");
  // Only the selected512-file profile changes density. Other existing envelope fixtures retain256.
  const repetitions = files === 512 ? qualificationFixture.repetitions : 256;
  const contents = `needle-${key} alpha beta\n`.repeat(repetitions);
  if (Buffer.byteLength(contents) > qualificationFixture.maxFileBytes)
    throw new Error("Qualification file exceeds fixed density bound");
  return contents;
}

/** Preserve original logical variants/hot-key work; change only resident admission order. */
export function qualificationVariant(key: number): WorkspaceIndexVariant {
  return key % 2 === 0 ? "paths" : "content";
}
export function qualificationResidentOrder(count: 2 | 3): number[] {
  const paths = Array.from({ length: 2 * count }, (_, index) => 2 * index);
  const content = paths.map((key) => key + 1);
  return [
    ...paths.slice(0, count),
    ...content.slice(0, count),
    ...paths.slice(count),
    ...content.slice(count),
  ];
}

export function assertBalancedCohorts(
  cohorts: readonly (readonly { key: { variant: WorkspaceIndexVariant } }[])[],
) {
  if (
    cohorts.some(
      (cohort) =>
        cohort.length !== 4 ||
        cohort.filter((handle) => handle.key.variant === "paths").length !== 2 ||
        cohort.filter((handle) => handle.key.variant === "content").length !== 2,
    )
  )
    throw new Error("Captured native process cohort lacks two path and two content finders");
}
