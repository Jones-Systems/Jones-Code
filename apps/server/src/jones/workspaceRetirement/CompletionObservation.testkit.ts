// @effect-diagnostics nodeBuiltinImport:off -- Bounded child fixtures prove deadline/reaping and no default content walk.
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import { fixture } from "./Conformance.testkit.ts";
import {
  completionObservation,
  recordCompletion,
  availableLineage,
} from "./CompletionObservation.ts";
import { outputObligations } from "./ProducerFinalizer.ts";
import { participantGuard } from "./ParticipantGuard.ts";
import { qualify } from "./Qualification.ts";

// The record is the behavior under test, not worker start latency. The production
// 100 ms bound is still proved by the stalled-capture case below.
const RECORD_TEST_DEADLINE_MS = 10_000;
const STALLED_WORKER_START_DEADLINE_MS = 2_000;

export const completionCases: Record<string, () => Promise<void>> = {
  "default completion records outcomes and lineage without walking large output descriptors":
    async () =>
      fixture(async ({ base, worktree, input }) => {
        const invalidRoot = NodePath.join(base, "not-a-directory");
        await NodeFSP.writeFile(invalidRoot, "large-tree descriptor; not traversable");
        for (const status of ["succeeded", "failed", "cancelled"] as const) {
          const observation = completionObservation({
            producer: "fixture",
            owner: "thread",
            worktree,
            status,
            roots: [NodePath.join(invalidRoot, "500000-entries")],
            lineage: {
              threadId: "thread",
              taskId: "task",
              pr: "https://github.com/example/project/pull/1",
            },
          });
          const result = recordCompletion(observation, undefined, RECORD_TEST_DEADLINE_MS);
          NodeAssert.equal(result.status, "recorded", result.reason ?? undefined);
          const stored = JSON.parse(await NodeFSP.readFile(result.reference!, "utf8"));
          NodeAssert.equal(stored.status, status);
          NodeAssert.equal(stored.proofStatus, "deferred");
          NodeAssert.equal(stored.lineage.threadId, "thread");
          NodeAssert.equal(stored.lineage.taskId, "task");
          NodeAssert.equal(stored.lineage.branch, "refs/heads/task");
          for (const prohibited of [
            "manifest",
            "content_manifest_sha256",
            "entries",
            "logical_bytes",
            "reconstructible",
          ])
            NodeAssert.equal(prohibited in stored, false);
          NodeAssert.equal(stored.schema, "workspace-output-observation/v1");
        }
        NodeAssert.deepEqual(await outputObligations(worktree), []);
        NodeAssert.equal(
          await NodeFSP.stat(NodePath.join(input.expected.git_dir, "workspace-outputs")).catch(
            () => null,
          ),
          null,
        );
        await NodeFSP.mkdir(NodePath.join(worktree, "build"));
        await NodeFSP.writeFile(NodePath.join(worktree, "build/note"), "unique note");
        const qualified = await qualify(input),
          detail = qualified.details?.find((d) => d.reason === "unattested_ignored");
        NodeAssert.equal(detail?.root, NodePath.join(worktree, "build"));
        NodeAssert.ok(detail?.missingFact);
        NodeAssert.ok(detail?.lineage.some((row) => row.threadId === "thread" && row.pr));
        NodeAssert.ok(detail?.nextAction.includes("must not manufacture"));
      }),
  "oversize capture starts no worker and stalled capture has no live work after its deadline":
    async () =>
      fixture(async ({ base, worktree }) => {
        const observation = completionObservation({
          producer: "fixture",
          owner: "thread",
          worktree,
          status: "succeeded",
          roots: [],
          lineage: {},
        });
        const marker = NodePath.join(base, "must-not-run");
        const oversize = recordCompletion(
          { ...observation, lineage: { oversize: "a".repeat(65536) } },
          `import{writeFileSync}from'node:fs';writeFileSync(${JSON.stringify(marker)},'bad');`,
        );
        NodeAssert.equal(oversize.reason, "metadata_bound");
        NodeAssert.equal(await NodeFSP.stat(marker).catch(() => null), null);
        // The default deadline holds whether or not the worker started before it expired.
        const started = performance.now();
        const stalled = recordCompletion(observation, "while(true){}");
        NodeAssert.equal(stalled.reason, "metadata_deadline");
        NodeAssert.ok(
          performance.now() - started < 1500,
          "deadline plus OS process close must remain bounded",
        );
        // Reaping needs a worker that has started, so give it a deadline it cannot miss on a slow host.
        const pidFile = NodePath.join(base, "worker.pid");
        const reaped = recordCompletion(
          observation,
          `import{writeFileSync}from'node:fs';writeFileSync(${JSON.stringify(pidFile)},String(process.pid));while(true){}`,
          STALLED_WORKER_START_DEADLINE_MS,
        );
        NodeAssert.equal(reaped.reason, "metadata_deadline");
        const pid = Number(await NodeFSP.readFile(pidFile, "utf8"));
        NodeAssert.throws(() => process.kill(pid, 0), { code: "ESRCH" });
        NodeAssert.deepEqual(await outputObligations(worktree), []);
      }),
  "unavailable and reserved sinks never invent allocation duties": async () =>
    fixture(async ({ worktree, input }) => {
      const observation = completionObservation({
        producer: "fixture",
        owner: "thread",
        worktree,
        status: "failed",
        roots: [],
        lineage: {},
      });
      NodeAssert.equal(recordCompletion(observation, "process.exitCode=1;").status, "unavailable");
      await participantGuard(input.expected, async () => {});
      const store = NodePath.join(
        input.expected.git_common_dir,
        "workspace-lifecycle/reservations.sqlite3",
      );
      const db = new NodeSqlite.DatabaseSync(store);
      try {
        db.exec("BEGIN IMMEDIATE");
        db.prepare("INSERT INTO reservations VALUES(?,?,?)").run(
          input.expected.admin_id,
          "other",
          "row",
        );
        db.exec("COMMIT");
      } finally {
        db.close();
      }
      NodeAssert.equal(recordCompletion(observation).status, "unavailable");
      NodeAssert.deepEqual(await outputObligations(worktree), []);
      NodeAssert.equal(
        await NodeFSP.stat(NodePath.join(input.expected.git_dir, "workspace-completions")).catch(
          () => null,
        ),
        null,
      );
    }),
  "lineage capture selects explicit known identifiers without copying other environment data":
    async () => {
      NodeAssert.deepEqual(
        availableLineage({
          T3CODE_THREAD_ID: "thread",
          T3CODE_TASK_ID: "task",
          T3CODE_RUN_ID: "run",
          T3CODE_ENVIRONMENT_ID: "environment",
          T3CODE_PROJECT_ID: "project",
          T3CODE_WORKSPACE_INCARNATION_ID: "incarnation",
          T3CODE_BRANCH: "branch",
          T3CODE_HEAD: "head",
          T3CODE_BASE_REF: "main",
          T3CODE_BASE_HEAD: "base",
          T3CODE_PREDECESSOR: "previous",
          T3CODE_PULL_REQUEST_URL: "pr",
          T3CODE_NATIVE_EVENT_ID: "event",
          PRIVATE_SECRET: "excluded",
        }),
        {
          threadId: "thread",
          taskId: "task",
          runId: "run",
          environmentId: "environment",
          projectId: "project",
          incarnationId: "incarnation",
          branch: "branch",
          head: "head",
          baseRef: "main",
          baseHead: "base",
          predecessor: "previous",
          pr: "pr",
          nativeEventId: "event",
        },
      );
    },
};
