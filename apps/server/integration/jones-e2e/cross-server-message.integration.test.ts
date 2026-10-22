import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  OrchestratorMcpThreadSendResult,
  ProjectId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";
import { createThread, failureOf, makeHome, startServer } from "./serverFixture.ts";

const decodeThreadSendResult = Schema.decodeUnknownEffect(OrchestratorMcpThreadSendResult);

it.effect("JE-08: linked MCP delivery commits once in B, never in A or unrelated C", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const a = yield* startServer(yield* makeHome("message-a"));
      const b = yield* startServer(yield* makeHome("message-b"));
      const c = yield* startServer(yield* makeHome("message-c"));
      expect(new Set([a.url, b.url, c.url]).size).toBe(3);
      expect(new Set([a.environmentId, b.environmentId, c.environmentId]).size).toBe(3);
      expect(new Set([a.config.dbPath, b.config.dbPath, c.config.dbPath]).size).toBe(3);
      expect(
        new Set([
          a.config.authorityStateDir,
          b.config.authorityStateDir,
          c.config.authorityStateDir,
        ]).size,
      ).toBe(3);

      const pairing = yield* b.auth.issuePairingCredential();
      const link = yield* a.links.link({
        url: b.url,
        pairingCode: pairing.credential,
        access: "auto",
      });
      expect(link.environmentId).toBe(b.environmentId);
      const sessions = (yield* b.auth.listSessions()).filter((session) =>
        session.client.label?.startsWith("T3 Code · "),
      );
      expect(sessions).toHaveLength(1);
      const linkedSession = sessions[0]!;
      const projectId = ProjectId.make("project:cross-server");
      // Deliberately collide IDs and titles across all databases to expose misrouting.
      const threadId = ThreadId.make("thread:cross-server");
      for (const server of [a, b, c]) {
        const workspaceRoot = `${server.config.baseDir}/workspace`;
        yield* fs.makeDirectory(workspaceRoot);
        yield* server.projects.create({
          commandId: CommandId.make("create:cross-server-project"),
          projectId,
          title: "Same project",
          workspaceRoot,
        });
        yield* createThread(server, {
          projectId,
          threadId,
          ...(server === b
            ? {
                linkOrigin: {
                  sessionId: linkedSession.sessionId,
                  label: linkedSession.client.label ?? "Synthetic link",
                  access: "auto" as const,
                },
              }
            : {}),
        });
      }
      const outside = ThreadId.make("thread:outside-link");
      yield* createThread(b, { projectId, threadId: outside });
      // Admit the caller's run through the real decider, without starting a provider worker.
      yield* a.threads.sendToThread({
        commandId: CommandId.make("je08-caller-run"),
        projectId,
        threadId,
        messageId: MessageId.make("je08-caller-prompt"),
        text: "Synthetic caller run; provider execution is disabled.",
        mode: "auto",
        attachments: [],
        createdBy: "user",
        creationSource: "web",
      });
      expect(yield* a.threads.getThreadShell(threadId)).toMatchObject({
        activeRunId: expect.any(String),
      });
      const beforeA = yield* a.orchestrator.getThreadProjection(threadId);
      const beforeC = yield* c.orchestrator.getThreadProjection(threadId);
      const receiptsA =
        yield* a.sql`SELECT * FROM orchestration_command_receipts ORDER BY command_id`;
      const beforeOutside = yield* b.orchestrator.getThreadProjection(outside);
      const input = {
        environmentId: b.environmentId,
        threadId,
        message: "JE-08 synthetic handoff: destination B only.",
        mode: "queue",
        clientRequestId: "je08-stable-delivery",
      };
      const sent = yield* a.call("t3_thread_send", input, threadId);
      expect(sent.isError).toBe(false);
      const result = yield* decodeThreadSendResult(sent.structuredContent);
      const committed = yield* b.orchestrator.getThreadProjection(threadId);
      expect(committed.messages).toHaveLength(1);
      expect(committed.messages[0]).toMatchObject({
        id: result.messageId,
        text: expect.stringContaining(input.message),
      });
      expect(committed.runs).toHaveLength(1);
      expect(committed.providerTurns).toHaveLength(0);
      const events = yield* b.sql`SELECT * FROM orchestration_events ORDER BY sequence`;
      const receipts =
        yield* b.sql`SELECT * FROM orchestration_command_receipts WHERE command_type = 'message.dispatch'`;
      expect(receipts).toHaveLength(1);
      expect(receipts[0]).toMatchObject({ aggregate_id: threadId, status: "accepted" });

      const retried = yield* a.call("t3_thread_send", input, threadId);
      expect(retried.isError).toBe(false);
      expect(retried.structuredContent).toEqual(sent.structuredContent);
      expect(yield* b.orchestrator.getThreadProjection(threadId)).toEqual(committed);
      expect(yield* b.sql`SELECT * FROM orchestration_events ORDER BY sequence`).toEqual(events);
      expect(
        yield* b.sql`SELECT * FROM orchestration_command_receipts WHERE command_type = 'message.dispatch'`,
      ).toEqual(receipts);

      const refused = yield* a.call(
        "t3_thread_send",
        {
          ...input,
          threadId: outside,
          clientRequestId: "je08-outside-link",
        },
        threadId,
      );
      expect(failureOf(refused)).toMatchObject({ code: "capability_denied" });
      const unlinked = yield* a.call(
        "t3_thread_send",
        {
          ...input,
          environmentId: c.environmentId,
          clientRequestId: "je08-unlinked",
        },
        threadId,
      );
      expect(failureOf(unlinked)).toMatchObject({ code: "invalid_request" });
      expect(yield* b.orchestrator.getThreadProjection(outside)).toEqual(beforeOutside);
      expect(yield* a.orchestrator.getThreadProjection(threadId)).toEqual(beforeA);
      expect(yield* c.orchestrator.getThreadProjection(threadId)).toEqual(beforeC);
      expect(yield* b.sql`SELECT * FROM orchestration_events ORDER BY sequence`).toEqual(events);
      expect(
        yield* a.sql`SELECT * FROM orchestration_command_receipts ORDER BY command_id`,
      ).toEqual(receiptsA);
      expect(
        yield* c.sql`SELECT command_id FROM orchestration_command_receipts WHERE command_type = 'message.dispatch'`,
      ).toEqual([]);
      expect((yield* a.orchestrator.getThreadProjection(threadId)).providerTurns).toEqual([]);
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);
