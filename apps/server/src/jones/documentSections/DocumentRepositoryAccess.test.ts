import { describe, expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationV2ThreadShell,
  type Project,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Invocation from "../../mcp/McpInvocationContext.ts";
import { liveThreadShell } from "../../mcp/McpToolAccess.testkit.ts";
import * as Threads from "../../orchestration-v2/ThreadManagementService.ts";
import * as Projects from "../../project/ProjectService.ts";
import { AuthorizedRepository } from "./AuthorizedRepository.ts";
import * as RepositoryAccess from "./DocumentRepositoryAccess.ts";

const ownId = ThreadId.make("own");
const otherId = ThreadId.make("other");
const owner: Invocation.McpInvocationScope = {
  environmentId: EnvironmentId.make("synthetic"),
  requestNamespace: "synthetic",
  issuedAt: 1,
  capabilities: new Set(["orchestration"]),
  thread: undefined,
  client: { sessionId: "owner", label: "Synthetic owner", access: "full-access" },
};
const own = { ...liveThreadShell(ownId), worktreePath: "/synthetic/own" };
const other = {
  ...liveThreadShell(otherId, { activeRunId: null }),
  worktreePath: "/synthetic/other",
};
const agent: Invocation.McpInvocationScope = {
  ...owner,
  client: undefined,
  thread: {
    threadId: ownId,
    providerInstanceId: own.providerInstanceId,
    providerSessionId: "credential",
  },
};

function resolve(
  scope: Invocation.McpInvocationScope,
  target?: typeof ownId,
  shells: readonly OrchestrationV2ThreadShell[] = [own, other],
) {
  return Effect.gen(function* () {
    const access = yield* RepositoryAccess.DocumentRepositoryAccess;
    return AuthorizedRepository.rootOf(yield* access.resolve(target));
  }).pipe(
    Effect.provide(
      RepositoryAccess.layer.pipe(
        Layer.provide(
          Layer.mock(Projects.ProjectService)({
            getById: () =>
              Effect.succeedSome({
                workspaceRoot: "/synthetic/project",
              } as Project),
          }),
        ),
      ),
    ),
    Effect.provideService(Invocation.McpInvocationContext, scope),
    Effect.provide(
      Layer.mock(Threads.ThreadManagementService)({
        getThreadShell: (id) => Effect.succeed(shells.find((shell) => shell.id === id) ?? null),
      }),
    ),
  );
}

describe("server-owned repository authorization", () => {
  it.effect(
    "selects only the agent's owning workspace, including when a different thread is explicitly requested",
    () =>
      Effect.gen(function* () {
        expect(yield* resolve(agent)).toBe("/synthetic/own");
        expect(yield* resolve(agent, ownId)).toBe("/synthetic/own");
        const error = yield* resolve(agent, otherId).pipe(Effect.flip);
        expect(error.code).toBe("capability_denied");
        expect(error.message).not.toContain("/synthetic/other");
      }),
  );
  it.effect(
    "requires owner target selection but permits an idle thread without starting a provider",
    () =>
      Effect.gen(function* () {
        const error = yield* resolve(owner).pipe(Effect.flip);
        expect(error.code).toBe("target_required");
        expect(yield* resolve(owner, otherId)).toBe("/synthetic/other");
        expect(yield* resolve(owner, otherId, [{ ...other, worktreePath: null }])).toBe(
          "/synthetic/project",
        );
      }),
  );
  it.effect("never falls back from stale agent credentials to an owner client", () =>
    Effect.gen(function* () {
      const forgedOwner = { ...agent, client: owner.client };
      const error = yield* resolve(forgedOwner, ownId, [{ ...own, activeRunId: null }]).pipe(
        Effect.flip,
      );
      expect(error.code).toBe("parent_not_active");
      const wrongProvider = {
        ...agent,
        thread: { ...agent.thread!, providerInstanceId: ProviderInstanceId.make("foreign") },
      };
      const stale = yield* resolve(wrongProvider, ownId).pipe(Effect.flip);
      expect(stale.code).toBe("parent_not_active");
      const archived = yield* resolve(agent, ownId, [{ ...own, archivedAt: own.createdAt }]).pipe(
        Effect.flip,
      );
      expect(archived.code).toBe("parent_not_active");
    }),
  );
  it.effect(
    "enforces authentication, capability, deletion, mode ceilings, and linked ownership",
    () =>
      Effect.gen(function* () {
        const missingAuth = yield* resolve({ ...owner, client: undefined }, ownId).pipe(
          Effect.flip,
        );
        expect(missingAuth.code).toBe("capability_denied");
        const missingCapability = yield* resolve({ ...owner, capabilities: new Set() }, ownId).pipe(
          Effect.flip,
        );
        expect(missingCapability.code).toBe("capability_denied");
        const deleted = yield* resolve(owner, ownId, [{ ...own, deletedAt: own.createdAt }]).pipe(
          Effect.flip,
        );
        expect(deleted.code).toBe("thread_not_found");
        const narrowed = {
          ...owner,
          client: {
            ...owner.client!,
            narrowedTo: {
              runtimeMode: "approval-required" as const,
              interactionMode: "plan" as const,
            },
          },
        };
        const modeError = yield* resolve(narrowed, ownId).pipe(Effect.flip);
        expect(modeError.code).toBe("runtime_mode_escalation_denied");
        const linked = {
          ...owner,
          client: {
            ...owner.client!,
            sessionId: "link-one",
            linked: true,
            access: "auto" as const,
          },
        };
        const target = { ...other, runtimeMode: "approval-required" as const };
        const linkError = yield* resolve(linked, otherId, [target]).pipe(Effect.flip);
        expect(linkError.code).toBe("capability_denied");
        expect(
          yield* resolve(linked, otherId, [
            {
              ...target,
              linkOrigin: {
                sessionId: "link-one",
                label: "Synthetic link",
                access: "auto",
              },
            },
          ]),
        ).toBe("/synthetic/other");
      }),
  );
  it.effect("allows read-only extraction within the client's existing mode ceiling", () =>
    Effect.gen(function* () {
      const readOnly = { ...owner, client: { ...owner.client!, access: "read-only" as const } };
      expect(
        yield* resolve(readOnly, otherId, [{ ...other, runtimeMode: "approval-required" }]),
      ).toBe("/synthetic/other");
    }),
  );
});
