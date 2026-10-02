// Native spawn captures only this adapter's child; the narrow runtime is injectable for synthetic checks.
// @effect-diagnostics nodeBuiltinImport:off
import * as NodeChildProcess from "node:child_process";
import * as NodeTimers from "node:timers";

import {
  TOKEN_ACCOUNTING_MAX_RESPONSE_BYTES,
  TokenAccountingReport,
  TokenAccountingUnavailable,
} from "@t3tools/contracts";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import {
  ProcessReaderConfiguration,
  processReaderFileSystem,
  processReaderIdentity,
  verifyProcessReaderConfiguration,
  type ProcessReaderFileSystem,
  type ProcessReaderIdentity,
} from "./ProcessReaderConfig.ts";
import {
  TOKEN_ACCOUNTING_READER_LIMITS,
  TokenAccountingReaderBinding,
  unconfiguredReader,
  type TokenAccountingReaderPort,
} from "./Reader.ts";

export interface ProcessReaderChild {
  readonly onStdout: (listener: (bytes: Uint8Array) => void) => () => void;
  readonly onStderr: (listener: (bytes: Uint8Array) => void) => () => void;
  readonly onError: (listener: () => void) => () => void;
  readonly onClose: (listener: (code: number | null) => void) => () => void;
  readonly kill: () => void;
  readonly destroyOutputs: () => void;
}
export interface ProcessReaderSpawnOptions {
  readonly cwd: string;
  readonly env: Readonly<Record<string, string>>;
  readonly shell: false;
  readonly windowsHide: true;
  readonly stdio: readonly ["ignore", "pipe", "pipe"];
}
export interface ProcessReaderRuntime {
  readonly fileSystem: ProcessReaderFileSystem;
  readonly identity: (
    signal: AbortSignal,
    platform: NodeJS.Platform,
  ) => Promise<ProcessReaderIdentity>;
  readonly spawn: (
    executable: string,
    args: readonly string[],
    options: ProcessReaderSpawnOptions,
  ) => ProcessReaderChild;
  readonly deadline: (milliseconds: number, callback: () => void) => () => void;
}

const nativeRuntime: ProcessReaderRuntime = {
  fileSystem: processReaderFileSystem,
  identity: processReaderIdentity,
  spawn: (executable, args, options) => {
    const child = NodeChildProcess.spawn(executable, args, {
      ...options,
      stdio: ["ignore", "pipe", "pipe"],
    });
    return {
      onStdout: (listener) => {
        child.stdout.on("data", listener);
        return () => {
          child.stdout.removeListener("data", listener);
        };
      },
      onStderr: (listener) => {
        child.stderr.on("data", listener);
        return () => {
          child.stderr.removeListener("data", listener);
        };
      },
      onError: (listener) => {
        child.once("error", listener);
        child.stdout.once("error", listener);
        child.stderr.once("error", listener);
        return () => {
          child.removeListener("error", listener);
          child.stdout.removeListener("error", listener);
          child.stderr.removeListener("error", listener);
        };
      },
      onClose: (listener) => {
        child.once("close", listener);
        return () => {
          child.removeListener("close", listener);
        };
      },
      kill: () => {
        child.kill("SIGKILL");
      },
      destroyOutputs: () => {
        child.stdout.destroy();
        child.stderr.destroy();
      },
    };
  },
  deadline: (milliseconds, callback) => {
    // This native callback deadline covers verification and shares the captured-child cleanup seam.
    // @effect-diagnostics-next-line globalTimers:off
    const timer = NodeTimers.setTimeout(callback, milliseconds);
    timer.unref();
    return () => {
      NodeTimers.clearTimeout(timer);
    };
  },
};

const decodeConfiguration = Schema.decodeUnknownSync(ProcessReaderConfiguration);
const decodeBinding = Schema.decodeUnknownSync(Schema.fromJsonString(TokenAccountingReaderBinding));
const decodeUnavailable = Schema.decodeUnknownSync(
  Schema.fromJsonString(TokenAccountingUnavailable),
);
const decodeReport = Schema.decodeUnknownSync(Schema.fromJsonString(TokenAccountingReport));
const bindingFailure = (reportId: string | null): TokenAccountingReaderBinding => ({
  status: "unconfigured",
  reason: "host_binding_unverified",
  configuredReportId: reportId,
});
const readerFailed: TokenAccountingUnavailable = {
  status: "reader_failed",
  reason: "reader_failed",
};
const hostUnverified: TokenAccountingUnavailable = {
  status: "unconfigured",
  reason: "host_binding_unverified",
};
const projectionInvalid: TokenAccountingUnavailable = {
  status: "invalid",
  reason: "projection_invalid",
};
const DEADLINE_MILLISECONDS = 5000;
const MAX_STDERR_BYTES = 16 * 1024;

function decodeReadResponse(text: string, reportId: string): string | TokenAccountingUnavailable {
  try {
    return decodeUnavailable(text);
  } catch {
    try {
      const report = decodeReport(text);
      return report.report_id === reportId
        ? text
        : { status: "invalid", reason: "configured_report_id_mismatch" };
    } catch {
      return projectionInvalid;
    }
  }
}

/** Only an explicitly enrolled server configuration can construct a subprocess reader. */
export function makeProcessReader(
  configuration?: ProcessReaderConfiguration,
  runtime: ProcessReaderRuntime = nativeRuntime,
): TokenAccountingReaderPort {
  if (configuration === undefined) return unconfiguredReader;
  let config: ProcessReaderConfiguration;
  try {
    config = Object.freeze(decodeConfiguration(configuration));
  } catch {
    return {
      checkBinding: Effect.succeed(bindingFailure(null)),
      readSummary: () => Effect.succeed(hostUnverified),
    };
  }

  const run = (operation: "check" | "read") =>
    HostProcessPlatform.pipe(
      Effect.flatMap((platform) =>
        Effect.callback<string | TokenAccountingUnavailable>((resume) => {
          const abort = new AbortController();
          let child: ProcessReaderChild | undefined;
          let childClosed = false;
          let finished = false;
          let cleaned = false;
          let cancelDeadline = () => {};
          const listeners: Array<() => void> = [];
          const chunks: Uint8Array[] = [];
          let stdoutBytes = 0;
          let stderrBytes = 0;
          const cleanup = () => {
            if (cleaned) return;
            cleaned = true;
            abort.abort();
            cancelDeadline();
            if (child !== undefined) {
              if (!childClosed) {
                try {
                  child.kill();
                } catch {
                  /* The captured child may already have exited. */
                }
              }
              child.destroyOutputs();
            }
            for (const remove of listeners.splice(0)) remove();
            chunks.length = 0;
          };
          const finish = (value: string | TokenAccountingUnavailable) => {
            if (finished) return;
            finished = true;
            cleanup();
            resume(Effect.succeed(value));
          };
          cancelDeadline = runtime.deadline(DEADLINE_MILLISECONDS, () =>
            finish({ status: "reader_failed", reason: "reader_timeout" }),
          );
          void verifyProcessReaderConfiguration(
            config,
            runtime.fileSystem,
            (signal) => runtime.identity(signal, platform),
            abort.signal,
          ).then(
            (binding) => {
              if (finished || abort.signal.aborted) return;
              try {
                child = runtime.spawn(
                  binding.python.path,
                  [
                    "-I",
                    "-B",
                    binding.helper.path,
                    "--binding",
                    config.bindingPath,
                    "--binding-sha256",
                    config.bindingSha256,
                    operation,
                    "--report-id",
                    config.reportId,
                  ],
                  {
                    cwd: binding.source_root,
                    env: { LANG: "C.UTF-8", LC_ALL: "C.UTF-8" },
                    shell: false,
                    windowsHide: true,
                    stdio: ["ignore", "pipe", "pipe"],
                  },
                );
                listeners.push(
                  child.onStdout((bytes) => {
                    if (finished || bytes.byteLength === 0) return;
                    stdoutBytes += bytes.byteLength;
                    if (stdoutBytes > TOKEN_ACCOUNTING_MAX_RESPONSE_BYTES) {
                      finish({ status: "oversized", reason: "projection_too_large" });
                      return;
                    }
                    chunks.push(bytes.slice());
                  }),
                );
                listeners.push(
                  child.onStderr((bytes) => {
                    stderrBytes += bytes.byteLength;
                    if (stderrBytes > MAX_STDERR_BYTES) finish(readerFailed);
                  }),
                );
                listeners.push(child.onError(() => finish(readerFailed)));
                listeners.push(
                  child.onClose((code) => {
                    childClosed = true;
                    if (code !== 0) {
                      finish(readerFailed);
                      return;
                    }
                    try {
                      const bytes = new Uint8Array(stdoutBytes);
                      let offset = 0;
                      for (const chunk of chunks) {
                        bytes.set(chunk, offset);
                        offset += chunk.byteLength;
                      }
                      finish(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
                    } catch {
                      finish(projectionInvalid);
                    }
                  }),
                );
              } catch {
                finish(readerFailed);
              }
            },
            () => finish(hostUnverified),
          );
          return Effect.sync(() => {
            finished = true;
            cleanup();
          });
        }),
      ),
    );

  return {
    checkBinding: run("check").pipe(
      Effect.map((response) => {
        if (typeof response !== "string") return bindingFailure(config.reportId);
        try {
          const checked = decodeBinding(response);
          return checked.configuredReportId === config.reportId ||
            (checked.status === "unconfigured" && checked.configuredReportId === null)
            ? checked
            : bindingFailure(config.reportId);
        } catch {
          return bindingFailure(config.reportId);
        }
      }),
    ),
    readSummary: ({ reportId, limits }) => {
      if (reportId !== config.reportId) {
        return Effect.succeed({
          status: "invalid",
          reason: "configured_report_id_mismatch",
        } as const);
      }
      if (
        Object.entries(TOKEN_ACCOUNTING_READER_LIMITS).some(
          ([key, value]) => limits[key as keyof typeof limits] !== value,
        )
      )
        return Effect.succeed(readerFailed);
      return run("read").pipe(
        Effect.map((response) =>
          typeof response === "string" ? decodeReadResponse(response, config.reportId) : response,
        ),
      );
    },
  };
}
