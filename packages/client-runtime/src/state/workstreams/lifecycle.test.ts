import { describe, expect, it } from "vite-plus/test";

import { resolveWorkstreamCompletionAuthority } from "./lifecycle.ts";

const declaration = (
  revision: number,
  lifecycle: "active" | "completed",
  workstreamId = "workstream-one",
) => {
  const lifecycleDeclaration = {
    lifecycle_declaration_id: `declaration-${revision}`,
    workstream_id: workstreamId,
    revision,
    prior_lifecycle: lifecycle === "completed" ? ("active" as const) : ("completed" as const),
    new_lifecycle: lifecycle,
    recorded_at: `2026-09-20T12:00:0${revision}Z`,
    actor: { principal_id: "owner-principal" },
    command_id: `lifecycle-command-${revision}`,
    supersedes_lifecycle_declaration_id: lifecycle === "active" ? "declaration-1" : null,
    registry_version: revision,
  };
  return {
    event_id: `event-${revision}`,
    command_id: lifecycleDeclaration.command_id,
    actor: lifecycleDeclaration.actor,
    operation: "update_workstream" as const,
    occurred_at: lifecycleDeclaration.recorded_at,
    registry_version: lifecycleDeclaration.registry_version,
    changed: true,
    workstream_versions: [{ workstream_id: workstreamId, version: revision }],
    native_reference_id: null,
    membership_ids: [],
    declaration_id: null,
    declaration_revision: null,
    edge_id: null,
    lifecycle_declaration: lifecycleDeclaration,
  };
};

const history = (items: ReturnType<typeof declaration>[], registryVersion = 10) => ({
  coverage: "complete" as const,
  context: { registry_version: registryVersion },
  items,
  next_cursor: null,
});

describe("workstream completion authority", () => {
  it("does not infer completion from evidence when lifecycle is active", () => {
    expect(
      resolveWorkstreamCompletionAuthority(
        { workstream_id: "workstream-one", lifecycle: "active" },
        history([declaration(1, "completed")]),
      ),
    ).toEqual({ state: "not-complete", lifecycle: "active" });
  });

  it("requires a matching owner lifecycle declaration for completion", () => {
    expect(
      resolveWorkstreamCompletionAuthority(
        { workstream_id: "workstream-one", lifecycle: "completed" },
        history([declaration(1, "completed")]),
      ),
    ).toMatchObject({
      state: "owner-declared",
      declaration: {
        actor: { principal_id: "owner-principal" },
        command_id: "lifecycle-command-1",
      },
    });
  });

  it("fails closed when completion has no matching declaration", () => {
    expect(
      resolveWorkstreamCompletionAuthority(
        { workstream_id: "workstream-one", lifecycle: "completed" },
        history([declaration(1, "completed", "another-workstream")]),
      ),
    ).toEqual({ state: "unverified", lifecycle: "completed", reason: "missing-declaration" });
  });

  it("fails closed when the latest declaration conflicts with the summary", () => {
    expect(
      resolveWorkstreamCompletionAuthority(
        { workstream_id: "workstream-one", lifecycle: "completed" },
        history([declaration(2, "active"), declaration(1, "completed")]),
      ),
    ).toEqual({ state: "unverified", lifecycle: "completed", reason: "conflicting-declaration" });
  });

  it("fails closed when the supplied history is incomplete", () => {
    expect(
      resolveWorkstreamCompletionAuthority(
        { workstream_id: "workstream-one", lifecycle: "completed" },
        {
          ...history([declaration(1, "completed")]),
          coverage: "partial",
        } as unknown as Parameters<typeof resolveWorkstreamCompletionAuthority>[1],
      ),
    ).toEqual({ state: "unverified", lifecycle: "completed", reason: "incomplete-history" });
  });

  it("fails closed on duplicate highest revisions regardless of order", () => {
    const completed = declaration(2, "completed");
    const active = declaration(2, "active");
    for (const items of [
      [completed, active],
      [active, completed],
    ]) {
      expect(
        resolveWorkstreamCompletionAuthority(
          { workstream_id: "workstream-one", lifecycle: "completed" },
          history(items),
        ),
      ).toEqual({ state: "unverified", lifecycle: "completed", reason: "conflicting-declaration" });
    }
  });

  it("fails closed when declaration evidence conflicts with its audit envelope", () => {
    const event = declaration(1, "completed");
    expect(
      resolveWorkstreamCompletionAuthority(
        { workstream_id: "workstream-one", lifecycle: "completed" },
        history([{ ...event, command_id: "different-command" }]),
      ),
    ).toEqual({ state: "unverified", lifecycle: "completed", reason: "conflicting-declaration" });
  });

  it("fails closed when declaration evidence is newer than its registry snapshot", () => {
    expect(
      resolveWorkstreamCompletionAuthority(
        { workstream_id: "workstream-one", lifecycle: "completed" },
        history([declaration(2, "completed")], 1),
      ),
    ).toEqual({ state: "unverified", lifecycle: "completed", reason: "conflicting-declaration" });
  });
});
