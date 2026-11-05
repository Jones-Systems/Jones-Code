import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { WorktreeMcpStatusResult } from "../worktreeMcp.ts";

describe("workspace provenance compatibility", () => {
  const legacy = {
    attached: false,
    worktreePath: null,
    branch: null,
    projectWorkspaceRoot: "/repo",
    defaultStartFromOrigin: false,
  };
  it("decodes older status results without claiming provenance availability", () => {
    const status = Schema.decodeSync(WorktreeMcpStatusResult)(legacy);
    expect(status).toEqual(legacy);
    expect(status.provenance).toBeUndefined();
  });
  it("retains detached and historical provenance while older status fields stay valid", () => {
    const id = { version: 1, device: "device", inode: "inode", birth: "birth" };
    const input = {
      ...legacy,
      provenance: [
        {
          schema: "workspace-provenance/v1",
          incarnationId: "workspace:one",
          environmentId: "environment",
          projectId: "project",
          path: "/old-detached",
          branch: null,
          head: "a".repeat(40),
          reason: "reference",
          createdAt: "2026-11-05T12:00:00.000Z",
          createdBy: "system",
          baseRef: null,
          baseHead: null,
          birth: id,
          adminId: id,
          predecessor: null,
          threads: ["one", "two"],
          tasks: ["task"],
          facts: [
            {
              id: "merge",
              kind: "pr_merge",
              recordedAt: "2026-11-05T12:00:00.000Z",
              reference: "github.com/Jones-Systems/Synthetic#1",
            },
          ],
          authorityEffect: "none",
        },
      ],
    };
    const status = Schema.decodeUnknownSync(WorktreeMcpStatusResult)(input);
    expect(status.provenance?.[0]?.branch).toBeNull();
    expect(status.provenance?.[0]?.threads).toEqual(["one", "two"]);
    expect(Schema.decodeSync(WorktreeMcpStatusResult)(status)).toEqual(status);
  });
});
