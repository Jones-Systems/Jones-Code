import { ProjectId, ProviderInstanceId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { serializeComposerMessageForServer } from "./composerContext";

import {
  buildProjectThreadStartTurnInput,
  deriveThreadTitleFromPrompt,
  type ProjectThreadStartTurnSpec,
} from "./projectThreadStartTurn";

describe("project thread worktree bootstrap", () => {
  const spec = {
    projectId: ProjectId.make("project"),
    projectCwd: "/workspace",
    threadId: "new-thread",
    commandId: "command",
    messageId: "message",
    createdAt: "2026-09-01T00:00:00Z",
    text: "Start the task",
    uploadedAttachments: [],
    modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.6-sol" },
    runtimeMode: "full-access",
    interactionMode: "default",
    workspaceMode: "worktree",
    branch: null,
    worktreePath: null,
    startFromOrigin: false,
    worktreeBranchName: "t3-task",
  } satisfies ProjectThreadStartTurnSpec;

  it.each([true, false, undefined])(
    "omits the automatic base and keeps the capability hint outside bootstrap (%s)",
    (serverResolvesWorktreeBase) => {
      const input = buildProjectThreadStartTurnInput({ ...spec, serverResolvesWorktreeBase });

      expect(input.bootstrap.prepareWorktree).toEqual({
        projectCwd: spec.projectCwd,
        branch: spec.worktreeBranchName,
      });
      expect(input.bootstrap.createThread.branch).toBeNull();
      expect(input.bootstrap.runSetupScript).toBe(true);
      expect(input.serverResolvesWorktreeBase).toBe(serverResolvesWorktreeBase);
      expect(input.bootstrap).not.toHaveProperty("serverResolvesWorktreeBase");
      expect(input.commandId).toBe(spec.commandId);
      expect(input.threadId).toBe(spec.threadId);
      expect(input.message.messageId).toBe(spec.messageId);
      expect(input.createdAt).toBe(spec.createdAt);
    },
  );

  it("preserves the chosen base and independent origin flag", () => {
    const input = buildProjectThreadStartTurnInput({
      ...spec,
      branch: "upstream/release",
      startFromOrigin: true,
    });

    expect(input.bootstrap.prepareWorktree).toEqual({
      projectCwd: spec.projectCwd,
      baseBranch: "upstream/release",
      branch: spec.worktreeBranchName,
      startFromOrigin: true,
    });
    expect(input.bootstrap.createThread.branch).toBe("upstream/release");
  });

  it("preserves the live local checkout without requesting worktree preparation", () => {
    const input = buildProjectThreadStartTurnInput({
      ...spec,
      workspaceMode: "local",
      branch: "feature/current",
      worktreePath: "/workspace/checkout",
    });

    expect(input.bootstrap.createThread).toMatchObject({
      branch: "feature/current",
      worktreePath: "/workspace/checkout",
    });
    expect(input.bootstrap).not.toHaveProperty("prepareWorktree");
    expect(input.bootstrap).not.toHaveProperty("runSetupScript");
  });
});

describe("project thread title", () => {
  it.each([
    {
      type: "image" as const,
      name: "photo.png",
      mimeType: "image/png",
      text: "![photo.png](t3-context://v1/image/photo)",
      title: "Attached image",
    },
    {
      type: "file" as const,
      name: "report.pdf",
      mimeType: "application/pdf",
      text: "",
      title: "File: report.pdf",
    },
    {
      type: "file" as const,
      name: "report.pdf",
      mimeType: "application/pdf",
      text: "[report.pdf](t3-context://v1/file/report)",
      title: "report.pdf",
    },
  ])("titles a $type-only message without changing its data", ({ text, title, ...attachment }) => {
    const uploadedAttachments = [{ ...attachment, id: "prepared-attachment", sizeBytes: 3 }];
    const wire = serializeComposerMessageForServer(text, undefined, true);
    const spec = {
      projectId: ProjectId.make("project"),
      projectCwd: "/workspace",
      threadId: "image-thread",
      commandId: "image-command",
      messageId: "image-message",
      createdAt: "2026-09-04T00:00:00Z",
      ...wire,
      titleText: text,
      uploadedAttachments,
      modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.6-sol" },
      runtimeMode: "full-access",
      interactionMode: "default",
      workspaceMode: "local",
      branch: null,
      worktreePath: null,
      startFromOrigin: false,
      worktreeBranchName: "unused",
    } satisfies ProjectThreadStartTurnSpec;
    const input = buildProjectThreadStartTurnInput(spec);

    expect(input.titleSeed).toBe(title);
    expect(input.bootstrap.createThread.title).toBe(input.titleSeed);
    expect(input.message.attachments).toEqual(uploadedAttachments);
    expect(input.message.text).toBe(wire.text);
    const relaunched = buildProjectThreadStartTurnInput({
      ...spec,
      threadId: "local-relaunch",
      text: input.message.text,
      titleText: input.bootstrap.createThread.title,
    });
    expect(relaunched.titleSeed).toBe(title);
    expect(relaunched.message.text).toBe(wire.text);
    expect(relaunched.message.attachments).toEqual(uploadedAttachments);
  });
});
