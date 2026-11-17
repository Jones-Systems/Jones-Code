import { OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { McpInvocationContext } from "../../../mcp/McpInvocationContext.ts";
import * as McpToolAccess from "../../../mcp/McpToolAccess.ts";
import { assertLiveCaller, readCaller } from "../../../mcp/threadAccess.ts";
import * as HostFiles from "../../hostFiles/HostFiles.ts";
import { HostFilesToolkit } from "./tools.ts";

const toFailure = (error: HostFiles.HostFileError) =>
  new OrchestratorMcpFailure({
    code:
      error.reason === "permission_denied"
        ? "capability_denied"
        : error.reason === "operation_failed"
          ? "orchestration_error"
          : "invalid_request",
    message:
      error.reason === "too_large" && error.sizeBytes !== undefined
        ? `${error.message} It is ${error.sizeBytes} bytes.`
        : error.message,
  });

/**
 * These tools reach any path this environment's user can, with no workspace
 * boundary, so only a caller that could already do that here may use them: a
 * full-access thread, or a client approved for full access. A forwarded call
 * arrives as the link's client narrowed to the calling agent's modes, so the
 * same check holds on the machine that owns the file.
 */
export const HostFilesHandlersLive = McpToolAccess.toLayer(HostFilesToolkit, {
  t3_file_read: McpToolAccess.reads((input) =>
    Effect.gen(function* () {
      const caller = yield* readCaller();
      yield* assertLiveCaller(caller);
      if (caller.limits.runtimeMode !== "full-access")
        return yield* new OrchestratorMcpFailure({
          code: "capability_denied",
          message: "Reading files through T3 needs a full-access calling thread or client.",
        });
      const files = yield* HostFiles.HostFiles;
      const view = yield* files.read(input).pipe(Effect.mapError(toFailure));
      return { ...view, environmentId: caller.scope.environmentId };
    }),
  ),
  t3_file_write: McpToolAccess.writesEnvironment((input) =>
    Effect.gen(function* () {
      const scope = yield* McpInvocationContext;
      const files = yield* HostFiles.HostFiles;
      const written = yield* files.write(input).pipe(Effect.mapError(toFailure));
      return { ...written, environmentId: scope.environmentId };
    }),
  ),
});
