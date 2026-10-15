import * as NodeServices from "@effect/platform-node/NodeServices";
// @effect-diagnostics nodeBuiltinImport:off - Exercises the real inherited descriptor lifecycle in a subprocess.
import * as NodeChildProcess from "node:child_process";
import * as NodeURL from "node:url";

import { expect, it } from "@effect/vitest";
import { PreviewAutomationRuntimeIdentity } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

const key = { threadId: "thread-1", tabId: "tab-1" };
const decodeRuntimeEvidence = Schema.decodeUnknownEffect(
  Schema.fromJsonString(
    Schema.Struct({
      attached: Schema.Boolean,
      evidence: Schema.NullOr(
        Schema.Struct({
          runtimeIdentity: Schema.NullOr(PreviewAutomationRuntimeIdentity),
          attachmentGeneration: Schema.Number,
        }),
      ),
    }),
  ),
);

const runtimeEvidenceOver = (events: ReadonlyArray<Record<string, unknown>>) =>
  Effect.gen(function* () {
    const child = yield* Effect.acquireRelease(
      Effect.sync(() =>
        NodeChildProcess.spawn(
          process.execPath,
          [
            NodeURL.fileURLToPath(
              new URL("./testing/DesktopPipeLifecycle.fixture.ts", import.meta.url),
            ),
            "--runtime-evidence",
          ],
          { stdio: ["ignore", "pipe", "pipe", "pipe", "pipe", "pipe", "pipe"] },
        ),
      ),
      (child) =>
        Effect.sync(() => {
          if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
          for (const stream of child.stdio) stream?.destroy();
        }),
    );
    const result = yield* Effect.tryPromise(
      () =>
        new Promise<string>((resolve, reject) => {
          let output = "";
          let errors = "";
          let evidence: string | undefined;
          child.stderr?.on("data", (chunk: Buffer) => {
            errors += chunk.toString();
          });
          child.stdout?.on("data", (chunk: Buffer) => {
            output += chunk.toString();
            let end: number;
            while ((end = output.indexOf("\n")) >= 0) {
              const line = output.slice(0, end);
              output = output.slice(end + 1);
              if (line === "ready") {
                const input = child.stdio[3];
                if (!input || !("write" in input)) {
                  reject(new Error("Missing desktop browser input pipe"));
                  return;
                }
                input.write(events.map((event) => `${JSON.stringify(event)}\n`).join(""));
              } else if (line.startsWith("runtime-evidence:")) {
                evidence = line.slice("runtime-evidence:".length);
              }
            }
          });
          child.once("error", reject);
          child.once("close", (code, signal) => {
            if (code !== 0 || signal !== null || evidence === undefined) {
              reject(new Error(`Desktop evidence fixture failed (${code}, ${signal}): ${errors}`));
              return;
            }
            resolve(evidence);
          });
        }),
    );
    return yield* decodeRuntimeEvidence(result);
  });

it("receives desktop messages and exits while the parent keeps both input pipes open", async () => {
  const child = NodeChildProcess.spawn(
    process.execPath,
    [NodeURL.fileURLToPath(new URL("./testing/DesktopPipeLifecycle.fixture.ts", import.meta.url))],
    { stdio: ["ignore", "pipe", "pipe", "pipe", "pipe", "pipe", "pipe"] },
  );
  // A failed exit must not leave a test process running. This never fires on success.
  // @effect-diagnostics-next-line globalTimers:off -- Bounds the native subprocess on failure; success waits for process exit.
  const watchdog = setTimeout(() => child.kill("SIGKILL"), 8_000);
  let output = "";
  let errors = "";
  let verified = false;
  child.stderr?.on("data", (chunk: Buffer) => {
    errors += chunk.toString();
  });
  const write = (fd: number, value: Record<string, unknown>) => {
    const stream = child.stdio[fd];
    if (!stream || !("write" in stream)) throw new Error(`Missing pipe ${fd}`);
    stream.write(`${JSON.stringify(value)}\n`);
  };
  const key = { threadId: "thread-1", tabId: "tab-1" };
  child.stdout?.on("data", (chunk: Buffer) => {
    output += chunk.toString();
    let end: number;
    while ((end = output.indexOf("\n")) >= 0) {
      const line = output.slice(0, end);
      output = output.slice(end + 1);
      if (line === "ready") {
        write(3, { type: "attached", ...key });
        write(5, { version: 1, type: "desktopTelemetryHello", electronPid: process.pid });
      } else if (line === "attached") {
        write(3, { type: "detached", ...key });
      } else if (line === "verified") {
        verified = true;
      }
    }
  });
  try {
    const result = await new Promise<{ code: number | null; signal: NodeJS.Signals | null }>(
      (resolve, reject) => {
        child.once("error", reject);
        child.once("exit", (code, signal) => resolve({ code, signal }));
      },
    );
    expect(errors).not.toContain("Error");
    expect(verified).toBe(true);
    expect(result).toEqual({ code: 0, signal: null });
  } finally {
    clearTimeout(watchdog);
    if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
    for (const stream of child.stdio) stream?.destroy();
  }
});

it.layer(NodeServices.layer)("DesktopBrowserChannel runtime evidence", (it) => {
  it.effect.each(["legacy", "replacement", "detached"] as const)(
    "keeps %s attachment evidence current without inventing runtime identity",
    (mode) =>
      Effect.gen(function* () {
        const identity = {
          schemaVersion: 1,
          runtimeKind: "electron",
          runtimeInstanceId: "native-runtime",
          appVersion: "0.1.0",
          buildCommit: null,
        } as const;
        const sentinel = { threadId: "sentinel-thread", tabId: "sentinel-tab" };
        const result = yield* runtimeEvidenceOver([
          { type: "attached", ...key, runtimeIdentity: identity },
          ...(mode === "detached"
            ? [{ type: "detached", ...key }]
            : [
                {
                  type: "attached",
                  ...key,
                  ...(mode === "replacement" ? { runtimeIdentity: identity } : {}),
                },
              ]),
          { type: "attached", ...sentinel },
        ]);
        // The sentinel follows the replacement/detach in the same owned fd stream.
        expect(result.attached).toBe(true);
        const evidence = result.evidence;
        expect(evidence).toEqual(
          mode === "detached"
            ? null
            : {
                runtimeIdentity: mode === "legacy" ? null : identity,
                attachmentGeneration: 2,
              },
        );
      }).pipe(Effect.scoped),
  );
});
