import { describe, expect, it } from "vite-plus/test";
import { EnvironmentId, ProviderInstanceId, ThreadId, type OrchestrationV2ThreadShell, type Project } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Invocation from "../../mcp/McpInvocationContext.ts";
import { liveThreadShell } from "../../mcp/McpToolAccess.testkit.ts";
import * as Threads from "../../orchestration-v2/ThreadManagementService.ts";
import * as Projects from "../../project/ProjectService.ts";
import { AuthorizedRepository } from "./AuthorizedRepository.ts";
import * as RepositoryAccess from "./DocumentRepositoryAccess.ts";

const ownId = ThreadId.make("own");
const otherId = ThreadId.make("other");
const owner: Invocation.McpInvocationScope = { environmentId: EnvironmentId.make("synthetic"),
  requestNamespace: "synthetic", issuedAt: 1, capabilities: new Set(["orchestration"]), thread: undefined,
  client: { sessionId: "owner", label: "Synthetic owner", access: "full-access" } };
const own = { ...liveThreadShell(ownId), worktreePath: "/synthetic/own" };
const other = { ...liveThreadShell(otherId, { activeRunId: null }), worktreePath: "/synthetic/other" };
const agent: Invocation.McpInvocationScope = { ...owner, client: undefined,
  thread: { threadId: ownId, providerInstanceId: own.providerInstanceId, providerSessionId: "credential" } };

function resolve(scope: Invocation.McpInvocationScope, target?: typeof ownId,
  shells: readonly OrchestrationV2ThreadShell[] = [own, other]) {
  return Effect.gen(function* () {
    const access = yield* RepositoryAccess.DocumentRepositoryAccess;
    return AuthorizedRepository.rootOf(yield* access.resolve(target));
  }).pipe(
    Effect.provide(RepositoryAccess.layer.pipe(Layer.provide(
      Layer.mock(Projects.ProjectService)({ getById: () => Effect.succeed(Option.some({
        workspaceRoot: "/synthetic/project",
      } as Project)) }),
    ))),
    Effect.provideService(Invocation.McpInvocationContext, scope),
    Effect.provide(Layer.mock(Threads.ThreadManagementService)({
      getThreadShell: (id) => Effect.succeed(shells.find((shell) => shell.id === id) ?? null),
    })),
  );
}

describe("server-owned repository authorization", () => {
  it("selects only the agent's owning workspace, including when a different thread is explicitly requested", async () => {
    expect(await Effect.runPromise(resolve(agent))).toBe("/synthetic/own");
    expect(await Effect.runPromise(resolve(agent, ownId))).toBe("/synthetic/own");
    const error = await Effect.runPromise(resolve(agent, otherId).pipe(Effect.flip));
    expect(error.code).toBe("capability_denied");
    expect(error.message).not.toContain("/synthetic/other");
  });
  it("requires owner target selection but permits an idle thread without starting a provider", async () => {
    const error = await Effect.runPromise(resolve(owner).pipe(Effect.flip));
    expect(error.code).toBe("target_required");
    expect(await Effect.runPromise(resolve(owner, otherId))).toBe("/synthetic/other");
    expect(await Effect.runPromise(resolve(owner, otherId, [{ ...other, worktreePath: null }])))
      .toBe("/synthetic/project");
  });
  it("never falls back from stale agent credentials to an owner client", async () => {
    const forgedOwner = { ...agent, client: owner.client };
    const error = await Effect.runPromise(resolve(forgedOwner, ownId, [{ ...own, activeRunId: null }])
      .pipe(Effect.flip));
    expect(error.code).toBe("parent_not_active");
    const wrongProvider = { ...agent, thread: { ...agent.thread!, providerInstanceId: ProviderInstanceId.make("foreign") } };
    const stale = await Effect.runPromise(resolve(wrongProvider, ownId).pipe(Effect.flip));
    expect(stale.code).toBe("parent_not_active");
    const archived = await Effect.runPromise(resolve(agent, ownId, [{ ...own, archivedAt: own.createdAt }])
      .pipe(Effect.flip));
    expect(archived.code).toBe("parent_not_active");
  });
  it("enforces authentication, capability, deletion, mode ceilings, and linked ownership", async () => {
    const missingAuth = await Effect.runPromise(resolve({ ...owner, client: undefined }, ownId).pipe(Effect.flip));
    expect(missingAuth.code).toBe("capability_denied");
    const missingCapability = await Effect.runPromise(resolve({ ...owner, capabilities: new Set() }, ownId).pipe(Effect.flip));
    expect(missingCapability.code).toBe("capability_denied");
    const deleted = await Effect.runPromise(resolve(owner, ownId, [{ ...own, deletedAt: own.createdAt }]).pipe(Effect.flip));
    expect(deleted.code).toBe("thread_not_found");
    const narrowed = { ...owner, client: { ...owner.client!, narrowedTo: {
      runtimeMode: "approval-required" as const, interactionMode: "plan" as const,
    } } };
    const modeError = await Effect.runPromise(resolve(narrowed, ownId).pipe(Effect.flip));
    expect(modeError.code).toBe("runtime_mode_escalation_denied");
    const linked = { ...owner, client: { ...owner.client!, sessionId: "link-one", linked: true,
      access: "auto" as const } };
    const target = { ...other, runtimeMode: "approval-required" as const };
    const linkError = await Effect.runPromise(resolve(linked, otherId, [target]).pipe(Effect.flip));
    expect(linkError.code).toBe("capability_denied");
    expect(await Effect.runPromise(resolve(linked, otherId, [{ ...target, linkOrigin: {
      sessionId: "link-one", label: "Synthetic link", access: "auto",
    } }]))).toBe("/synthetic/other");
  });
  it("allows read-only extraction within the client's existing mode ceiling", async () => {
    const readOnly = { ...owner, client: { ...owner.client!, access: "read-only" as const } };
    expect(await Effect.runPromise(resolve(readOnly, otherId, [{ ...other, runtimeMode: "approval-required" }])))
      .toBe("/synthetic/other");
  });
});
