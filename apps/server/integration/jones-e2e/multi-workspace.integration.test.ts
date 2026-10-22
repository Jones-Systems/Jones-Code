import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { CommandId, MessageId, ProjectId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import { createThread, makeHome, startServer, type TestServer } from "./serverFixture.ts";

it.effect("JE-09: workspace placement survives closing and reopening the server's disk state", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const home = yield* makeHome("workspaces");
      const placements = [
        {
          projectId: ProjectId.make("project:workspace-a"),
          threadId: ThreadId.make("thread:workspace-a"),
          workspaceRoot: `${home}/workspace-a`,
        },
        {
          projectId: ProjectId.make("project:workspace-b"),
          threadId: ThreadId.make("thread:workspace-b"),
          workspaceRoot: `${home}/workspace-b`,
        },
      ] as const;
      for (const placement of placements) {
        yield* fs.makeDirectory(placement.workspaceRoot);
        yield* fs.writeFileString(`${placement.workspaceRoot}/sentinel.txt`, placement.projectId);
      }
      const readPlacement = (server: TestServer) =>
        Effect.gen(function* () {
          const rows = [];
          for (const placement of placements) {
            const project = Option.getOrThrow(yield* server.projects.getById(placement.projectId));
            const thread = yield* server.threads.getProjectThreadRecords(placement, []);
            expect(project.workspaceRoot).toBe(placement.workspaceRoot);
            expect(thread.thread.projectId).toBe(placement.projectId);
            expect(thread.thread.worktreePath).toBeNull();
            const listed = yield* server.threads.listProjectThreads({
              projectId: placement.projectId,
              includeSubagents: true,
            });
            expect(listed.map((shell) => shell.id)).toEqual([placement.threadId]);
            rows.push({ project, thread });
          }
          return rows;
        });
      const first = yield* Effect.scoped(
        Effect.gen(function* () {
          const server = yield* startServer(home);
          for (const placement of placements) {
            yield* server.projects.create({
              ...placement,
              commandId: CommandId.make(`create:${placement.projectId}`),
              title: "Same project title",
            });
            yield* createThread(server, placement);
          }
          return {
            environmentId: server.environmentId,
            config: server.config,
            placement: yield* readPlacement(server),
          };
        }),
      );
      expect(yield* fs.exists(first.config.serverRuntimeStatePath)).toBe(false);
      expect(yield* fs.exists(first.config.dbPath)).toBe(true);

      yield* Effect.scoped(
        Effect.gen(function* () {
          const server = yield* startServer(home);
          expect(server.environmentId).toBe(first.environmentId);
          expect(yield* readPlacement(server)).toEqual(first.placement);
          const [a, b] = placements;
          const wrongProject = { projectId: b.projectId, threadId: a.threadId };
          const read = yield* server.threads
            .getProjectThreadRecords(wrongProject, [])
            .pipe(Effect.result);
          expect(read._tag).toBe("Failure");
          if (read._tag === "Failure")
            expect(read.failure._tag).toBe("ThreadManagementThreadNotFoundError");
          const commandId = CommandId.make("je09-wrong-project-send");
          const sent = yield* server.threads
            .sendToThread({
              ...wrongProject,
              commandId,
              messageId: MessageId.make("je09-wrong-project-message"),
              text: "Must never reach either workspace",
              mode: "queue",
              attachments: [],
              createdBy: "agent",
              creationSource: "mcp",
            })
            .pipe(Effect.result);
          expect(sent._tag).toBe("Failure");
          if (sent._tag === "Failure")
            expect(sent.failure._tag).toBe("ThreadManagementThreadNotFoundError");
          expect(Option.isNone(yield* server.receipts.getByCommandId(commandId))).toBe(true);
          for (const placement of placements) {
            expect(
              (yield* server.orchestrator.getThreadProjection(placement.threadId)).messages,
            ).toEqual([]);
            expect(yield* fs.readFileString(`${placement.workspaceRoot}/sentinel.txt`)).toBe(
              placement.projectId,
            );
          }
          expect(yield* readPlacement(server)).toEqual(first.placement);
        }),
      );
      expect(yield* fs.exists(first.config.serverRuntimeStatePath)).toBe(false);
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);
