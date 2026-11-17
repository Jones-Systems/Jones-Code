import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  OrchestratorMcpEnvironmentTarget,
  OrchestratorMcpFailure,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import { Tool } from "effect/ai";

import * as Invocation from "../../../mcp/McpInvocationContext.ts";
import * as McpToolAccess from "../../../mcp/McpToolAccess.ts";
import * as ThreadManagementService from "../../../orchestration-v2/ThreadManagementService.ts";
import * as PeerForwarding from "../../../peer/PeerForwarding.ts";
import * as HostFiles from "../../hostFiles/HostFiles.ts";
import { HostFilesHandlersLive } from "./handlers.ts";
import { HostFilesToolkit } from "./tools.ts";

const here = EnvironmentId.make("files-environment");
const there = EnvironmentId.make("linked-files-environment");

const clientScope = (
  client: Partial<Invocation.McpClientCaller> & Pick<Invocation.McpClientCaller, "access">,
): Invocation.McpInvocationScope => ({
  environmentId: here,
  thread: undefined,
  client: { sessionId: "synthetic-session", label: "synthetic", ...client },
  capabilities: new Set(["orchestration"]),
  issuedAt: 0,
  requestNamespace: "synthetic-files",
});

/** Stands in for the disk, so each case can see whether the handler reached it. */
const disk = () => {
  const calls: Array<{ readonly operation: "read" | "write"; readonly input: unknown }> = [];
  return {
    calls,
    layer: Layer.succeed(HostFiles.HostFiles, {
      read: (input) => {
        calls.push({ operation: "read", input });
        return Effect.succeed({
          kind: "file",
          path: input.path,
          content: "contents",
          startLine: 1,
          totalLines: 1,
          nextStartLine: null,
          sizeBytes: 8,
          modifiedAt: null,
        });
      },
      write: (input) => {
        calls.push({ operation: "write", input });
        return input.path.endsWith("taken.txt")
          ? Effect.fail(new HostFiles.HostFileError({ reason: "already_exists", path: input.path }))
          : Effect.succeed({ path: input.path, sizeBytes: input.content.length, created: true });
      },
    }),
  };
};

const forwardingTo = (calls: Array<unknown>) =>
  Layer.mock(PeerForwarding.PeerForwarding)({
    call: (_caller, tool, environmentId, input) => {
      calls.push({ name: tool.name, environmentId, input });
      return Effect.fail(
        new OrchestratorMcpFailure({ code: "orchestration_error", message: "Synthetic remote" }),
      );
    },
  });

const handle = (
  name: keyof typeof HostFilesToolkit.tools,
  input: Record<string, unknown>,
  scope: Invocation.McpInvocationScope,
  files: Layer.Layer<HostFiles.HostFiles>,
  forwarded: Array<unknown> = [],
) =>
  Effect.gen(function* () {
    const toolkit = yield* HostFilesToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(HostFilesHandlersLive).pipe(
          Layer.provide(forwardingTo(forwarded)),
        ),
      ),
    );
    const results = yield* toolkit
      .handle(name, input as never)
      .pipe(Stream.unwrap, Stream.runCollect);
    return results.at(-1)?.result;
  }).pipe(
    Effect.provideService(Invocation.McpInvocationContext, scope),
    Effect.provide(files),
    Effect.provide(Layer.mock(ThreadManagementService.ThreadManagementService)({})),
  );

it("declares both tools as forwardable, with only the read marked read-only", () => {
  const { t3_file_read: read, t3_file_write: write } = HostFilesToolkit.tools;
  expect(Object.keys(HostFilesToolkit.tools)).toEqual(["t3_file_read", "t3_file_write"]);
  expect(read.parametersSchema.fields.environmentId).toBe(OrchestratorMcpEnvironmentTarget);
  expect(write.parametersSchema.fields.environmentId).toBe(OrchestratorMcpEnvironmentTarget);
  expect(Tool.getJsonSchema(read)).toMatchObject({ type: "object" });
  expect(Tool.getJsonSchema(write)).toMatchObject({ type: "object" });
});

it.effect("a full-access client reads and writes files in this environment", () =>
  Effect.gen(function* () {
    const source = disk();
    const scope = clientScope({ access: "full-access" });
    expect(
      yield* handle("t3_file_read", { path: "/srv/notes.txt", maxLines: 5 }, scope, source.layer),
    ).toMatchObject({ kind: "file", environmentId: here, content: "contents" });
    expect(
      yield* handle("t3_file_write", { path: "/srv/new.txt", content: "abc" }, scope, source.layer),
    ).toEqual({ environmentId: here, path: "/srv/new.txt", sizeBytes: 3, created: true });
    expect(source.calls).toEqual([
      { operation: "read", input: { path: "/srv/notes.txt", maxLines: 5 } },
      { operation: "write", input: { path: "/srv/new.txt", content: "abc" } },
    ]);
  }),
);

it.effect("callers below full access never reach the disk", () =>
  Effect.gen(function* () {
    const source = disk();
    const below = [
      clientScope({ access: "read-only" }),
      clientScope({ access: "auto" }),
      // A link paired with full access, calling for an agent that runs below it.
      clientScope({
        access: "full-access",
        linked: true,
        narrowedTo: { runtimeMode: "auto", interactionMode: "default" },
      }),
    ];
    for (const scope of below) {
      expect(
        yield* handle("t3_file_read", { path: "/srv/notes.txt" }, scope, source.layer),
      ).toMatchObject({ _tag: "OrchestratorMcpFailure", code: "capability_denied" });
      expect(
        yield* handle("t3_file_write", { path: "/srv/new.txt", content: "x" }, scope, source.layer),
      ).toMatchObject({ _tag: "OrchestratorMcpFailure", code: "capability_denied" });
    }
    expect(
      yield* handle(
        "t3_file_read",
        { path: "/srv/notes.txt" },
        { ...clientScope({ access: "full-access" }), capabilities: new Set() },
        source.layer,
      ),
    ).toMatchObject({ _tag: "OrchestratorMcpFailure", code: "capability_denied" });
    expect(source.calls).toEqual([]);
  }),
);

it.effect("a plan-mode full-access caller may read but not write", () =>
  Effect.gen(function* () {
    const source = disk();
    const scope = clientScope({
      access: "full-access",
      narrowedTo: { runtimeMode: "full-access", interactionMode: "plan" },
    });
    expect(
      yield* handle("t3_file_read", { path: "/srv/notes.txt" }, scope, source.layer),
    ).toMatchObject({ kind: "file" });
    expect(
      yield* handle("t3_file_write", { path: "/srv/new.txt", content: "x" }, scope, source.layer),
    ).toMatchObject({ _tag: "OrchestratorMcpFailure", code: "capability_denied" });
    expect(source.calls.map((call) => call.operation)).toEqual(["read"]);
  }),
);

it.effect("reports a refused file operation as a request error with its fixed reason text", () =>
  Effect.gen(function* () {
    const source = disk();
    expect(
      yield* handle(
        "t3_file_write",
        { path: "/srv/taken.txt", content: "x" },
        clientScope({ access: "full-access" }),
        source.layer,
      ),
    ).toEqual(
      new OrchestratorMcpFailure({
        code: "invalid_request",
        message: "A file already exists there. Pass overwrite: true to replace it.",
      }),
    );
  }),
);

it.effect("sends a call that names a linked environment there and leaves this disk alone", () =>
  Effect.gen(function* () {
    const source = disk();
    const forwarded: Array<unknown> = [];
    const scope = clientScope({ access: "full-access" });
    const read = { environmentId: there, path: "/srv/notes.txt" };
    const write = { environmentId: there, path: "/srv/new.txt", content: "abc", overwrite: true };
    expect(yield* handle("t3_file_read", read, scope, source.layer, forwarded)).toMatchObject({
      message: "Synthetic remote",
    });
    expect(yield* handle("t3_file_write", write, scope, source.layer, forwarded)).toMatchObject({
      message: "Synthetic remote",
    });
    // A write is refused here before it leaves; the linked environment rechecks a read itself.
    expect(
      yield* handle(
        "t3_file_write",
        write,
        clientScope({ access: "auto" }),
        source.layer,
        forwarded,
      ),
    ).toMatchObject({ code: "capability_denied" });
    expect(forwarded).toEqual([
      { name: "t3_file_read", environmentId: there, input: read },
      { name: "t3_file_write", environmentId: there, input: write },
    ]);
    expect(source.calls).toEqual([]);
  }),
);
