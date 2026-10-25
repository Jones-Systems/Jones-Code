// @effect-diagnostics nodeBuiltinImport:off -- The frozen V3 contract uses absolute lexical paths, independently of filesystem observations.
import * as NodePath from "node:path";
import * as Schema from "effect/Schema";

const text = (max: number) =>
  Schema.String.check(
    Schema.makeFilter(
      (s) => s.trim().length > 0 && s.length <= max && [...s].every((c) => c.charCodeAt(0) >= 32),
    ),
  );
const path = text(4096).check(
  Schema.makeFilter((s) => NodePath.isAbsolute(s) && NodePath.normalize(s) === s),
);
const branch = text(1024).check(
  Schema.makeFilter(
    (s) =>
      s.startsWith("refs/heads/") &&
      s !== "refs/heads/main" &&
      s !== "refs/heads/master" &&
      !/[ ~^:?*[\\]/u.test(s) &&
      !s.includes("..") &&
      !s.includes("//") &&
      !s.includes("@{") &&
      !/[/.]$/u.test(s) &&
      !s.split("/").some((p) => p.startsWith(".") || p.endsWith(".lock")),
  ),
);
const bindingFields = {
  environment_id: text(1024),
  thread_id: text(1024),
  incarnation_id: text(1024),
  repository: text(1024),
  worktree: path,
  git_common_dir: path,
  git_dir: path,
  branch,
  head: Schema.String.check(Schema.isPattern(/^(?:[a-f0-9]{40}|[a-f0-9]{64})$/u)),
  source_id: text(1024),
};
export const NativeBinding = Schema.Struct(bindingFields).check(
  Schema.makeFilter((b) => b.git_common_dir !== b.git_dir),
);
export const Binding = Schema.Struct({ ...bindingFields, source_generation: text(1024) }).check(
  Schema.makeFilter((b) => b.git_common_dir !== b.git_dir),
);
export type Binding = typeof Binding.Type;
const references = Schema.Array(text(2048)).check(Schema.isMaxLength(32));
export const Assessment = Schema.Struct({
  schema: Schema.Literal("workspace-assessment/v1"),
  binding: Binding,
  role: Schema.Literals(["authoring", "execution"]),
  disposition: Schema.Literals([
    "active",
    "needs_handoff",
    "preserve",
    "cleanup_candidate",
    "unknown",
  ]),
  remaining_work: references,
  preservation_refs: references,
  handoff_refs: references,
  artifact_destinations: references,
  unresolved_obligations: references,
}).check(
  Schema.makeFilter(
    (a) =>
      a.disposition !== "cleanup_candidate" ||
      (a.remaining_work.length === 0 && a.unresolved_obligations.length === 0),
  ),
);
export const InventoryEntry = Schema.Struct({
  worktree: path,
  generation: Schema.NullOr(text(1024)),
  binding: Schema.NullOr(Binding),
  assessment: Schema.NullOr(
    Schema.Struct({
      schema: Schema.Literal("workspace-assessment-record/v1"),
      generation: text(1024),
      assessment: Assessment,
      recorded_at: text(1024),
      binding_evidence: Schema.Literal("caller_asserted"),
      authority_effect: Schema.Literal("none"),
    }),
  ),
  disposition: Assessment.fields.disposition,
  assessment_status: Schema.Literals(["current", "stale", "absent"]),
  stored_readiness: Schema.NullOr(
    Schema.Struct({
      schema: Schema.Literal("worktree-readiness/v1"),
      worktree: path,
      outcome: Schema.Literals(["ready", "invalidated", "unknown"]),
    }),
  ),
  physical_readiness: Schema.Literal("unknown"),
  binding_evidence: Schema.Literal("caller_asserted"),
  authority_effect: Schema.Literal("none"),
});
export type InventoryEntry = typeof InventoryEntry.Type;

export class LifecycleError extends Schema.TaggedError<LifecycleError>()(
  "WorkspaceLifecycleError",
  {
    reason: Schema.Literals([
      "unavailable",
      "identity",
      "stale",
      "unknown",
      "conflict",
      "preserve",
      "transaction",
      "storage",
    ]),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Workspace lifecycle held: ${this.reason}.`;
  }
}
