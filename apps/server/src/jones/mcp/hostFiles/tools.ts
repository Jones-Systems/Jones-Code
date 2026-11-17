import {
  HostFileReadInput,
  HostFileReadResult,
  HostFileWriteInput,
  HostFileWriteResult,
  OrchestratorMcpFailure,
} from "@t3tools/contracts";
import { Tool, Toolkit } from "effect/ai";
import { McpInvocationContext } from "../../../mcp/McpInvocationContext.ts";
import * as ThreadManagementService from "../../../orchestration-v2/ThreadManagementService.ts";
import * as HostFiles from "../../hostFiles/HostFiles.ts";

const shared = {
  failure: OrchestratorMcpFailure,
  failureMode: "return" as const,
  dependencies: [
    McpInvocationContext,
    ThreadManagementService.ThreadManagementService,
    HostFiles.HostFiles,
  ],
};

export const HostFilesToolkit = Toolkit.make(
  Tool.make("t3_file_read", {
    ...shared,
    description:
      "Read a text file by line window, or list a directory, on this machine or a linked one (`environmentId`). `nextStartLine` continues a long file. Full-access callers only.",
    parameters: HostFileReadInput,
    success: HostFileReadResult,
  })
    .annotate(Tool.Readonly, true)
    .annotate(Tool.Destructive, false)
    .annotate(Tool.Idempotent, true)
    .annotate(Tool.OpenWorld, false),
  Tool.make("t3_file_write", {
    ...shared,
    description:
      "Write a text file on this machine or a linked one (`environmentId`), creating parent directories. An existing file needs `overwrite: true`. Full-access/default callers only.",
    parameters: HostFileWriteInput,
    success: HostFileWriteResult,
  })
    .annotate(Tool.Readonly, false)
    .annotate(Tool.Destructive, true)
    .annotate(Tool.Idempotent, false)
    .annotate(Tool.OpenWorld, false),
);
