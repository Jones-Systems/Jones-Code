import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import {
  OrchestrationV2ClientCommand,
  OrchestrationV2Command,
  OrchestrationV2RpcSchemas,
} from "./orchestrationV2.ts";
import { ProjectMutation } from "./project.ts";
import { CommandId, ProjectId, ThreadId } from "./baseSchemas.ts";
import { ProviderInstanceId } from "./providerInstance.ts";
import {
  DEFAULT_PROVIDER_INTERACTION_MODE,
  DEFAULT_RUNTIME_MODE,
  ProjectIconOverride,
  ThreadTurnStartCommand,
  type ChatImageAttachment,
  ChatAttachment,
  UploadChatAttachment,
} from "./orchestration.ts";

const decodeThreadTurnStartCommand = Schema.decodeUnknownEffect(ThreadTurnStartCommand);
function getOptionValue(
  options: ReadonlyArray<{ id: string; value: unknown }> | undefined,
  id: string,
): unknown {
  return options?.find((option) => option.id === id)?.value;
}

it.effect("decodes thread.turn.start defaults for provider and runtime mode", () =>
  Effect.gen(function* () {
    const parsed = yield* decodeThreadTurnStartCommand({
      type: "thread.turn.start",
      commandId: "cmd-turn-1",
      threadId: "thread-1",
      message: {
        messageId: "msg-1",
        role: "user",
        text: "hello",
        attachments: [],
      },
      createdAt: "2026-01-01T00:00:00.000Z",
    });
    assert.strictEqual(parsed.modelSelection, undefined);
    assert.strictEqual(parsed.runtimeMode, DEFAULT_RUNTIME_MODE);
    assert.strictEqual(parsed.interactionMode, DEFAULT_PROVIDER_INTERACTION_MODE);
  }),
);

it.effect("preserves window capture metadata in thread.turn.start", () =>
  Effect.gen(function* () {
    const parsed = yield* decodeThreadTurnStartCommand({
      type: "thread.turn.start",
      commandId: "cmd-snap-shot",
      threadId: "thread-1",
      message: {
        messageId: "msg-snap-shot",
        role: "user",
        text: "Review this window",
        attachments: [
          {
            type: "image",
            id: "snap-shot-1",
            name: "editor.png",
            mimeType: "image/png",
            sizeBytes: 4,
            source: {
              kind: "snap-shot",
              capturedAt: "2026-08-24T11:00:00.000Z",
              appName: "Editor",
              windowTitle: "main.ts",
              accessibleText: "const answer = 42;",
              accessibility: {
                format: "element-tree",
                coordinateSpace: "captured-image",
                imageSize: { width: 800, height: 600 },
                truncated: false,
                root: {
                  role: "window",
                  name: "main.ts",
                  bounds: { x: 0, y: 0, width: 800, height: 600 },
                  children: [
                    {
                      role: "text",
                      value: "const answer = 42;",
                      bounds: { x: 20, y: 40, width: 180, height: 20 },
                      children: [],
                    },
                  ],
                },
              },
              appIdentifier: "com.example.editor",
              appIconDataUrl: "data:image/png;base64,iVBORw==",
            },
          },
        ],
      },
      createdAt: "2026-08-24T11:00:00.000Z",
    });

    const attachment = parsed.message.attachments[0];
    assert.strictEqual(attachment?.type, "image");
    assert.deepStrictEqual((attachment as ChatImageAttachment).source, {
      kind: "snap-shot",
      capturedAt: "2026-08-24T11:00:00.000Z",
      appName: "Editor",
      windowTitle: "main.ts",
      accessibleText: "const answer = 42;",
      accessibility: {
        format: "element-tree",
        coordinateSpace: "captured-image",
        imageSize: { width: 800, height: 600 },
        truncated: false,
        root: {
          role: "window",
          name: "main.ts",
          bounds: { x: 0, y: 0, width: 800, height: 600 },
          children: [
            {
              role: "text",
              value: "const answer = 42;",
              bounds: { x: 20, y: 40, width: 180, height: 20 },
              children: [],
            },
          ],
        },
      },
      appIdentifier: "com.example.editor",
      appIconDataUrl: "data:image/png;base64,iVBORw==",
    });
  }),
);

it.effect("preserves explicit provider and runtime mode in thread.turn.start", () =>
  Effect.gen(function* () {
    const parsed = yield* decodeThreadTurnStartCommand({
      type: "thread.turn.start",
      commandId: "cmd-turn-2",
      threadId: "thread-1",
      message: {
        messageId: "msg-2",
        role: "user",
        text: "hello",
        attachments: [],
      },
      modelSelection: {
        provider: "codex",
        model: "gpt-5.4",
      },
      runtimeMode: "full-access",
      createdAt: "2026-01-01T00:00:00.000Z",
    });
    assert.strictEqual(parsed.modelSelection?.instanceId, "codex");
    assert.strictEqual(parsed.runtimeMode, "full-access");
    assert.strictEqual(parsed.interactionMode, DEFAULT_PROVIDER_INTERACTION_MODE);
  }),
);

it.effect("accepts bootstrap metadata in thread.turn.start", () =>
  Effect.gen(function* () {
    const parsed = yield* decodeThreadTurnStartCommand({
      type: "thread.turn.start",
      commandId: "cmd-turn-bootstrap",
      threadId: "thread-1",
      message: {
        messageId: "msg-bootstrap",
        role: "user",
        text: "hello",
        attachments: [],
      },
      bootstrap: {
        createThread: {
          projectId: "project-1",
          title: "Bootstrap thread",
          modelSelection: {
            provider: "codex",
            model: "gpt-5.4",
          },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          createdAt: "2026-01-01T00:00:00.000Z",
        },
        prepareWorktree: {
          projectCwd: "/tmp/workspace",
          baseBranch: "main",
          branch: "t3code/example",
          startFromOrigin: true,
          requireWorktree: true,
        },
        runSetupScript: true,
      },
      createdAt: "2026-01-01T00:00:00.000Z",
    });
    assert.strictEqual(parsed.bootstrap?.createThread?.projectId, "project-1");
    assert.strictEqual(parsed.bootstrap?.prepareWorktree?.baseBranch, "main");
    assert.strictEqual(parsed.bootstrap?.prepareWorktree?.startFromOrigin, true);
    assert.strictEqual(parsed.bootstrap?.prepareWorktree?.requireWorktree, true);
    assert.strictEqual(parsed.bootstrap?.runSetupScript, true);
  }),
);

it.effect("accepts provider-scoped model options in thread.turn.start", () =>
  Effect.gen(function* () {
    const parsed = yield* decodeThreadTurnStartCommand({
      type: "thread.turn.start",
      commandId: "cmd-turn-options",
      threadId: "thread-1",
      message: {
        messageId: "msg-options",
        role: "user",
        text: "hello",
        attachments: [],
      },
      modelSelection: {
        provider: "codex",
        model: "gpt-5.3-codex",
        options: [
          { id: "reasoningEffort", value: "high" },
          { id: "fastMode", value: true },
        ],
      },
      createdAt: "2026-01-01T00:00:00.000Z",
    });
    assert.strictEqual(parsed.modelSelection?.instanceId, "codex");
    assert.strictEqual(getOptionValue(parsed.modelSelection?.options, "reasoningEffort"), "high");
    assert.strictEqual(getOptionValue(parsed.modelSelection?.options, "fastMode"), true);
  }),
);

it.effect("accepts a title seed in thread.turn.start", () =>
  Effect.gen(function* () {
    const parsed = yield* decodeThreadTurnStartCommand({
      type: "thread.turn.start",
      commandId: "cmd-turn-title-seed",
      threadId: "thread-1",
      message: {
        messageId: "msg-title-seed",
        role: "user",
        text: "hello",
        attachments: [],
      },
      titleSeed: "Investigate reconnect failures",
      createdAt: "2026-01-01T00:00:00.000Z",
    });
    assert.strictEqual(parsed.titleSeed, "Investigate reconnect failures");
  }),
);

it.effect("accepts a source proposed plan reference in thread.turn.start", () =>
  Effect.gen(function* () {
    const parsed = yield* decodeThreadTurnStartCommand({
      type: "thread.turn.start",
      commandId: "cmd-turn-source-plan",
      threadId: "thread-2",
      message: {
        messageId: "msg-source-plan",
        role: "user",
        text: "implement this",
        attachments: [],
      },
      sourceProposedPlan: {
        threadId: "thread-1",
        planId: "plan-1",
      },
      createdAt: "2026-01-01T00:00:00.000Z",
    });
    assert.deepStrictEqual(parsed.sourceProposedPlan, {
      threadId: "thread-1",
      planId: "plan-1",
    });
  }),
);

const decodeProjectIcon = Schema.decodeUnknownEffect(ProjectIconOverride);
const encodeProjectIcon = Schema.encodeEffect(ProjectIconOverride);

// Pre-monogram clients reject unknown variants; nightly clients additionally validate monogram.
const decodeOldIcon = Schema.decodeUnknownEffect(
  Schema.Union([
    Schema.Struct({ kind: Schema.Literal("lucide"), name: Schema.String, color: Schema.String }),
    Schema.Struct({ kind: Schema.Literal("emoji"), emoji: Schema.String }),
  ]),
);
const decodeNightlyIcon = Schema.decodeUnknownEffect(
  Schema.Union([
    Schema.Struct({
      kind: Schema.Literal("lucide"),
      name: Schema.String,
      color: Schema.String,
      // Fail if this field is ever sent; old validators must never see the new text.
      monogram: Schema.optional(Schema.Never),
    }),
    Schema.Struct({ kind: Schema.Literal("emoji"), emoji: Schema.String }),
  ]),
);

it.effect("sends monograms as fallback icons that old and nightly clients can decode", () =>
  Effect.gen(function* () {
    const fallback = { kind: "lucide", name: "folder-code", color: "violet" } as const;
    for (const text of ["T3", "क्ष्म", "e\u0301"]) {
      const monogram = { kind: "monogram", text, color: "violet" } as const;
      const wire = yield* encodeProjectIcon(monogram);
      assert.deepEqual(wire, { ...fallback, monogramText: text });
      assert.deepEqual(yield* decodeOldIcon(wire), fallback);
      assert.deepEqual(yield* decodeNightlyIcon(wire), fallback);
      assert.deepEqual(yield* decodeProjectIcon(wire), monogram);
      assert.deepEqual(yield* decodeProjectIcon(monogram), monogram);
      assert.deepEqual(yield* decodeProjectIcon({ ...fallback, monogram: text }), monogram);
    }
    for (const icon of [
      { kind: "lucide", name: "alarm-clock", color: "blue" },
      { kind: "emoji", emoji: "🚀" },
    ] as const) {
      assert.deepEqual(yield* decodeProjectIcon(icon), icon);
      assert.deepEqual(yield* encodeProjectIcon(icon), icon);
    }
  }),
);

const decodeV2Command = Schema.decodeUnknownEffect(OrchestrationV2Command);
const decodeProjectMutation = Schema.decodeUnknownEffect(ProjectMutation);

it.effect("decodes thread archive and unarchive commands", () =>
  Effect.gen(function* () {
    const archive = yield* decodeV2Command({
      type: "thread.archive",
      commandId: "cmd-archive-1",
      threadId: "thread-1",
    });
    const unarchive = yield* decodeV2Command({
      type: "thread.unarchive",
      commandId: "cmd-unarchive-1",
      threadId: "thread-1",
    });

    assert.strictEqual(archive.type, "thread.archive");
    assert.strictEqual(unarchive.type, "thread.unarchive");
  }),
);

it.effect("decodes thread settle and unsettle commands", () =>
  Effect.gen(function* () {
    const settle = yield* decodeV2Command({
      type: "thread.settle",
      commandId: "cmd-settle-1",
      threadId: "thread-1",
    });
    const unsettle = yield* decodeV2Command({
      type: "thread.unsettle",
      commandId: "cmd-unsettle-1",
      threadId: "thread-1",
      reason: "user",
    });

    assert.strictEqual(settle.type, "thread.settle");
    assert.strictEqual(unsettle.type, "thread.unsettle");

    // "activity" is server-owned: it exists on the event, never on the
    // command, so a client cannot forge the neutral reset.
    const forged = yield* decodeV2Command({
      type: "thread.unsettle",
      commandId: "cmd-unsettle-2",
      threadId: "thread-1",
      reason: "activity",
    }).pipe(Effect.flip);
    assert.ok(forged);
  }),
);

it.effect("accepts thread.pull-request.link and .unlink commands", () =>
  Effect.gen(function* () {
    const link = yield* decodeV2Command({
      type: "thread.pull-request.link",
      commandId: "cmd-link-pull-request",
      threadId: "thread-1",
      host: "github.com",
      repository: "pingdotgg/t3code",
      number: 42,
      url: "https://github.com/pingdotgg/t3code/pull/42",
      source: "manual",
    });
    assert.strictEqual(link.type, "thread.pull-request.link");
    if (link.type === "thread.pull-request.link") {
      assert.strictEqual(link.source, "manual");
      assert.strictEqual(link.number, 42);
    }

    const unlink = yield* decodeV2Command({
      type: "thread.pull-request.unlink",
      commandId: "cmd-unlink-pull-request",
      threadId: "thread-1",
      host: "github.com",
      repository: "pingdotgg/t3code",
      number: 42,
    });
    assert.strictEqual(unlink.type, "thread.pull-request.unlink");
  }),
);

it.effect("accepts a title regeneration intent in thread.metadata.update", () =>
  Effect.gen(function* () {
    const parsed = yield* decodeV2Command({
      type: "thread.metadata.update",
      commandId: "cmd-title-regenerate",
      threadId: "thread-1",
      regenerateTitle: true,
    });
    assert.strictEqual(parsed.type, "thread.metadata.update");
    if (parsed.type === "thread.metadata.update") {
      assert.strictEqual(parsed.regenerateTitle, true);
    }
  }),
);

it.effect("trims branded ids and command string fields at decode boundaries", () =>
  Effect.gen(function* () {
    const parsed = yield* decodeProjectMutation({
      type: "project.create",
      commandId: " cmd-1 ",
      projectId: " project-1 ",
      title: " Project Title ",
      workspaceRoot: " /tmp/workspace ",
      defaultModelSelection: {
        provider: "codex",
        model: " gpt-5.2 ",
      },
    });
    if (parsed.type !== "project.create") throw new Error("Unexpected command");
    assert.strictEqual(parsed.commandId, "cmd-1");
    assert.strictEqual(parsed.projectId, "project-1");
    assert.strictEqual(parsed.title, "Project Title");
    assert.strictEqual(parsed.workspaceRoot, "/tmp/workspace");
    assert.strictEqual(parsed.createWorkspaceRootIfMissing, undefined);
    assert.deepStrictEqual(parsed.defaultModelSelection, {
      instanceId: ProviderInstanceId.make("codex"),
      model: "gpt-5.2",
    });
  }),
);

it.effect("decodes project.create with createWorkspaceRootIfMissing enabled", () =>
  Effect.gen(function* () {
    const parsed = yield* decodeProjectMutation({
      type: "project.create",
      commandId: "cmd-1",
      projectId: "project-1",
      title: "Project Title",
      workspaceRoot: "/tmp/workspace",
      createWorkspaceRootIfMissing: true,
    });

    if (parsed.type !== "project.create") throw new Error("Unexpected command");
    assert.strictEqual(parsed.createWorkspaceRootIfMissing, true);
  }),
);

it.effect("rejects command fields that become empty after trim", () =>
  Effect.gen(function* () {
    const result = yield* Effect.exit(
      decodeProjectMutation({
        type: "project.create",
        commandId: "cmd-1",
        projectId: "project-1",
        title: "  ",
        workspaceRoot: "/tmp/workspace",
      }),
    );
    assert.strictEqual(result._tag, "Failure");
  }),
);

it.effect("preserves V2 active order keys and rejects empty reorder keys", () =>
  Effect.gen(function* () {
    const input = {
      type: "thread.active.reorder",
      commandId: "command-1",
      threadId: "thread-1",
      orderKey: "gm",
    };
    const command = yield* decodeV2Command(input);
    if (command.type !== "thread.active.reorder") throw new Error("Unexpected command");
    assert.strictEqual(command.orderKey, "gm");
    const failure = yield* Effect.exit(decodeV2Command({ ...input, orderKey: " " }));
    assert.strictEqual(failure._tag, "Failure");
  }),
);

it.effect("accepts an internal title regeneration completion", () =>
  Effect.gen(function* () {
    const parsed = yield* decodeV2Command({
      type: "thread.title.regeneration.complete",
      commandId: "cmd-title-regeneration-complete",
      threadId: "thread-1",
      requestId: "cmd-title-regenerate",
      title: "Updated title",
    });
    assert.strictEqual(parsed.type, "thread.title.regeneration.complete");
    if (parsed.type === "thread.title.regeneration.complete") {
      assert.strictEqual(parsed.requestId, "cmd-title-regenerate");
      assert.strictEqual(parsed.title, "Updated title");
    }
  }),
);

it.effect("accepts pull request synchronization only as an internal command", () =>
  Effect.gen(function* () {
    const pullRequest = {
      projectId: ProjectId.make("project-1"),
      repository: "pingdotgg/t3code",
      number: 42,
      url: "https://github.com/pingdotgg/t3code/pull/42",
    };
    const command = {
      type: "thread.pull-request.sync" as const,
      commandId: CommandId.make("cmd-pull-request-sync"),
      threadId: ThreadId.make("thread-1"),
      projectId: pullRequest.projectId,
      snapshotSequence: 12,
      expected: {
        workspaceRoot: "/workspace/project",
        branch: "feature",
        worktreePath: null,
        linkedPullRequest: null,
        branchPullRequest: null,
      },
      branchPullRequest: pullRequest,
      linkedPullRequest: pullRequest,
    };

    assert.deepStrictEqual(yield* decodeV2Command(command), command);
    assert.ok(
      yield* Schema.decodeUnknownEffect(OrchestrationV2ClientCommand)(command).pipe(Effect.flip),
    );
    assert.ok(
      yield* Schema.decodeUnknownEffect(OrchestrationV2RpcSchemas.dispatchCommand.input)(
        command,
      ).pipe(Effect.flip),
    );

    const cleared = { ...command, branchPullRequest: null };
    assert.deepStrictEqual(yield* decodeV2Command(cleared), cleared);

    const metadata = yield* decodeV2Command({
      type: "thread.metadata.update",
      commandId: "cmd-forged-branch-pull-request",
      threadId: "thread-1",
      branchPullRequest: pullRequest,
    });
    assert.isFalse("branchPullRequest" in metadata);
  }),
);

it.effect("rejects an explicit title combined with title regeneration", () =>
  Effect.gen(function* () {
    const result = yield* Effect.exit(
      decodeV2Command({
        type: "thread.metadata.update",
        commandId: "cmd-title-regenerate-with-title",
        threadId: "thread-1",
        title: "Explicit title",
        regenerateTitle: true,
      }),
    );
    assert.strictEqual(result._tag, "Failure");
  }),
);

it("preserves inline images, uploaded images and uploaded files at their shared attachment codecs", () => {
  const inline = {
    type: "image",
    name: "legacy.png",
    mimeType: "image/png",
    sizeBytes: 3,
    dataUrl: "data:image/png;base64,YWJj",
  };
  const uploaded = {
    type: "image",
    id: "pending-00000000-0000-4000-8000-000000000001",
    name: "uploaded.png",
    mimeType: "image/png",
    sizeBytes: 3,
  };
  const file = {
    type: "file",
    id: "pending-00000000-0000-4000-8000-000000000002-pdf",
    name: "report.pdf",
    mimeType: "application/pdf",
    sizeBytes: 3,
  };
  assert.deepEqual<unknown>(Schema.decodeUnknownSync(UploadChatAttachment)(inline), inline);
  assert.deepEqual<unknown>(Schema.decodeUnknownSync(ChatAttachment)(uploaded), uploaded);
  assert.deepEqual<unknown>(Schema.decodeUnknownSync(ChatAttachment)(file), file);
});
