import {
  TOKEN_ACCOUNTING_CAVEATS,
  TOKEN_ACCOUNTING_IDENTITY_ALGORITHM,
  TOKEN_ACCOUNTING_MAX_RESPONSE_BYTES,
  TOKEN_ACCOUNTING_PROJECTION_SCHEMA,
  TOKEN_ACCOUNTING_REPORT_SCHEMA,
  type TokenAccountingReport,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import { afterEach, expect, it, vi } from "@effect/vitest";

import {
  makeProcessReader,
  type ProcessReaderChild,
  type ProcessReaderRuntime,
} from "./ProcessReader.ts";
import * as Config from "./ProcessReaderConfig.ts";
import { TOKEN_ACCOUNTING_READER_LIMITS } from "./Reader.ts";

const reportId = "a".repeat(64);
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const config = { bindingPath: "/fixture/binding.json", bindingSha256: "b".repeat(64), reportId };
const binding: Config.ProcessReaderBinding = {
  schema: "programmatic-token-info.saved-accounting-binding/v1",
  uid: 42,
  machine_id_sha256: "c".repeat(64),
  python: { path: "/fixture/python", sha256: "d".repeat(64) },
  helper: { path: "/fixture/src/codex_v3/token_info/saved_reader.py", sha256: "e".repeat(64) },
  source_root: "/fixture/src",
  archive_root: "/fixture/archive",
  report_id: reportId,
  source_closure: Object.fromEntries(
    Config.TOKEN_ACCOUNTING_SOURCE_PATHS.map((path) => [path, "e".repeat(64)]),
  ) as Config.ProcessReaderBinding["source_closure"],
  source_closure_sha256: "f".repeat(64),
  authority_effect: "none",
};

// Synthetic projection identities prove transport only; the canonical Python helper proves source identity.
function report(): TokenAccountingReport {
  const metric = { known_sum: null, total: null, known_requests: 0, missing_requests: 0 };
  const metrics = {
    input_tokens: metric,
    cached_input_tokens: metric,
    cache_write_input_tokens: metric,
    cache_write_5m_tokens: metric,
    cache_write_1h_tokens: metric,
    output_tokens: metric,
    reasoning_output_tokens: metric,
  };
  const statuses = { primary: 0, legacy_unresolved: 0, conflict: 0, aggregate_delta: 0 };
  const partition = { requests: 0, metrics: { ...metrics, ordinary_input: metric } };
  const allocation = {
    allocated: metric,
    unknown: metric,
    groups: [],
    unknown_by_reason: {},
    estimated_coverage: null,
  };
  return {
    schema: TOKEN_ACCOUNTING_PROJECTION_SCHEMA,
    source_schema: TOKEN_ACCOUNTING_REPORT_SCHEMA,
    source_identity_algorithm: TOKEN_ACCOUNTING_IDENTITY_ALGORITHM,
    source_identity_verified: true,
    report_id: reportId,
    selection_id: "b".repeat(64),
    snapshot: { index_snapshot_id: "c".repeat(64) },
    window: { start: "2026-09-01T00:00:00Z", end: "2026-09-02T00:00:00Z", end_exclusive: true },
    provider_coverage: {
      selected_requests: 0,
      accounting_status_counts: statuses,
      token_missingness: {
        input_tokens: 0,
        cached_input_tokens: 0,
        cache_write_input_tokens: 0,
        cache_write_5m_tokens: 0,
        cache_write_1h_tokens: 0,
        output_tokens: 0,
        reasoning_output_tokens: 0,
      },
      latest_scan: {
        captured_at: null,
        selected_files: null,
        refreshed_selected_files: null,
        partial_selected_files: null,
        root_scope_ids: [],
        mtime_cutoff: null,
        receipts_status_counts: null,
        validated_files: null,
      },
      cumulative_index: {
        tracked_files: null,
        retained_not_refreshed_files: null,
        missing_tracked_files: null,
      },
      freshness: {
        status: "scan_unavailable",
        requested_end: null,
        last_scan_captured_at: null,
        selected_validation_min: null,
        selected_validation_max: null,
        unrefreshed_files: null,
      },
      historical_window_completeness: "not_proven",
    },
    provider_ledger: {
      requests: 0,
      primary_requests: 0,
      nonadditive_requests: 0,
      accounting_status_counts: statuses,
      metrics: { ...metrics, ordinary_input_tokens: metric, non_read_input_tokens: metric },
    },
    input: { ordinary_input: metric, ...allocation },
    output: { output: metric, measured_reasoning: metric, ...allocation },
    partitions: {
      provider: { codex: partition, claude: partition },
      role: { root: partition, child: partition, unknown: partition },
    },
    mechanism_flags: { nonadditive: true, counts: {} },
    estimator_status_counts: { qualified: 0, unavailable: 0, failed: 0 },
    coverage: {
      primary_requests: 0,
      captured_requests: 0,
      unavailable_requests: 0,
      complete_response_requests: 0,
      input_allocated_requests: 0,
      output_allocated_requests: 0,
      reasoning_measured_requests: 0,
      reasoning_missing_requests: 0,
      estimator_qualified_models: 0,
      estimator_unavailable_models: 0,
      diagnostics: {},
    },
    caveats: TOKEN_ACCOUNTING_CAVEATS,
    authority_effect: "none",
  };
}

function harness() {
  const stdout = new Set<(bytes: Uint8Array) => void>();
  const stderr = new Set<(bytes: Uint8Array) => void>();
  const errors = new Set<() => void>();
  const closes = new Set<(code: number | null) => void>();
  const deadlines = new Set<() => void>();
  const subscribe = <A>(listeners: Set<A>, listener: A) => {
    listeners.add(listener);
    return () => {
      listeners.delete(listener);
    };
  };
  const child: ProcessReaderChild = {
    onStdout: (listener) => subscribe(stdout, listener),
    onStderr: (listener) => subscribe(stderr, listener),
    onError: (listener) => subscribe(errors, listener),
    onClose: (listener) => subscribe(closes, listener),
    kill: vi.fn(),
    destroyOutputs: vi.fn(),
  };
  let resolveSpawn!: () => void;
  let resolveVerify!: () => void;
  let resolveScheduled!: () => void;
  const futureSpawns = new Set<() => void>();
  const spawned = new Promise<void>((resolve) => {
    resolveSpawn = resolve;
  });
  const verifying = new Promise<void>((resolve) => {
    resolveVerify = resolve;
  });
  const scheduled = new Promise<void>((resolve) => {
    resolveScheduled = resolve;
  });
  const verify = vi
    .spyOn(Config, "verifyProcessReaderConfiguration")
    .mockImplementation(async () => {
      resolveVerify();
      return binding;
    });
  const runtime: ProcessReaderRuntime = {
    fileSystem: {
      lstat: vi.fn().mockRejectedValue(new Error("unexpected file access")),
      open: vi.fn().mockRejectedValue(new Error("unexpected file access")),
    },
    identity: vi.fn().mockRejectedValue(new Error("unexpected host observation")),
    spawn: vi.fn(() => {
      resolveSpawn();
      for (const ready of futureSpawns) ready();
      futureSpawns.clear();
      return child;
    }),
    deadline: vi.fn((milliseconds: number, callback: () => void) => {
      expect(milliseconds).toBe(5000);
      resolveScheduled();
      return subscribe(deadlines, callback);
    }),
  };
  const emit = (listeners: Set<(bytes: Uint8Array) => void>, bytes: Uint8Array) => {
    for (const listener of listeners) listener(bytes);
  };
  const close = (code: number | null = 0) => {
    for (const listener of closes) listener(code);
  };
  return {
    runtime,
    child,
    verify,
    spawned,
    verifying,
    scheduled,
    nextSpawn: () =>
      new Promise<void>((resolve) => {
        futureSpawns.add(resolve);
      }),
    reader: () => makeProcessReader(config, runtime),
    stdout: (text: string) => emit(stdout, new TextEncoder().encode(text)),
    stdoutBytes: (bytes: Uint8Array) => emit(stdout, bytes),
    stderr: (bytes: Uint8Array) => emit(stderr, bytes),
    close,
    error: () => {
      for (const listener of errors) listener();
    },
    expire: () => {
      for (const callback of deadlines) callback();
    },
    reply: (value: unknown) => {
      emit(stdout, new TextEncoder().encode(encodeJson(value)));
      close();
    },
    listeners: () => stdout.size + stderr.size + errors.size + closes.size + deadlines.size,
  };
}
const read = (input: ReturnType<typeof harness>) =>
  input.reader().readSummary({ reportId, limits: TOKEN_ACCOUNTING_READER_LIMITS });

afterEach(() => {
  vi.restoreAllMocks();
});

it.layer(Layer.succeed(HostProcessPlatform, "linux"))(
  "fixed canonical accounting subprocess reader",
  (it) => {
    it.effect("defaults unconfigured with zero verification, files, timers or subprocesses", () =>
      Effect.gen(function* () {
        const input = harness();
        const reader = makeProcessReader(undefined, input.runtime);
        expect(yield* reader.checkBinding).toEqual({
          status: "unconfigured",
          reason: "reader_unconfigured",
          configuredReportId: null,
        });
        expect(
          yield* reader.readSummary({ reportId, limits: TOKEN_ACCOUNTING_READER_LIMITS }),
        ).toEqual({ status: "unconfigured", reason: "reader_unconfigured" });
        expect(input.verify).not.toHaveBeenCalled();
        expect(input.runtime.spawn).not.toHaveBeenCalled();
        expect(input.runtime.deadline).not.toHaveBeenCalled();
      }),
    );

    it.effect(
      "rejects malformed configuration and caller-selected reports or limits before verification",
      () =>
        Effect.gen(function* () {
          const input = harness();
          const bad = makeProcessReader(
            { ...config, command: "caller-command" } as never,
            input.runtime,
          );
          expect(yield* bad.checkBinding).toMatchObject({
            status: "unconfigured",
            reason: "host_binding_unverified",
          });
          expect(
            yield* input.reader().readSummary({
              reportId: "c".repeat(64),
              limits: TOKEN_ACCOUNTING_READER_LIMITS,
            }),
          ).toEqual({ status: "invalid", reason: "configured_report_id_mismatch" });
          expect(
            yield* input.reader().readSummary({
              reportId,
              limits: { ...TOKEN_ACCOUNTING_READER_LIMITS, maxInputBytes: 1 } as never,
            }),
          ).toEqual({ status: "reader_failed", reason: "reader_failed" });
          expect(input.verify).not.toHaveBeenCalled();
          expect(input.runtime.spawn).not.toHaveBeenCalled();
        }),
    );

    it.effect(
      "never spawns after failed custody or digest verification and returns no diagnostics",
      () =>
        Effect.gen(function* () {
          const input = harness();
          input.verify.mockRejectedValue(new Error("/synthetic/private/path"));
          expect(yield* input.reader().checkBinding).toEqual({
            status: "unconfigured",
            reason: "host_binding_unverified",
            configuredReportId: reportId,
          });
          expect(yield* read(input)).toEqual({
            status: "unconfigured",
            reason: "host_binding_unverified",
          });
          expect(input.runtime.spawn).not.toHaveBeenCalled();
          expect(input.listeners()).toBe(0);
        }),
    );

    it.effect(
      "spawns only the pinned interpreter with fixed argv, cwd, empty stdin and sanitized environment",
      () =>
        Effect.gen(function* () {
          const input = harness();
          const checked = yield* input.reader().checkBinding.pipe(Effect.forkChild);
          yield* Effect.promise(() => input.spawned);
          expect(input.verify).toHaveBeenCalledWith(
            config,
            input.runtime.fileSystem,
            expect.any(Function),
            expect.any(AbortSignal),
          );
          vi.mocked(input.runtime.identity).mockResolvedValue({
            realUid: 42,
            effectiveUid: 42,
            savedUid: 42,
          });
          const identify = vi.mocked(input.verify).mock.calls[0]![2];
          const signal = new AbortController().signal;
          yield* Effect.promise(() => identify(signal));
          expect(input.runtime.identity).toHaveBeenCalledExactlyOnceWith(signal, "linux");
          expect(input.runtime.spawn).toHaveBeenCalledExactlyOnceWith(
            binding.python.path,
            [
              "-I",
              "-B",
              binding.helper.path,
              "--binding",
              config.bindingPath,
              "--binding-sha256",
              config.bindingSha256,
              "check",
              "--report-id",
              reportId,
            ],
            {
              cwd: binding.source_root,
              env: { LANG: "C.UTF-8", LC_ALL: "C.UTF-8" },
              shell: false,
              windowsHide: true,
              stdio: ["ignore", "pipe", "pipe"],
            },
          );
          input.reply({ status: "bound", configuredReportId: reportId });
          expect(yield* Fiber.join(checked)).toEqual({
            status: "bound",
            configuredReportId: reportId,
          });
          expect(input.child.kill).not.toHaveBeenCalled();
          expect(input.listeners()).toBe(0);
        }),
    );

    it.effect(
      "rejects bound reply identity mismatch and newer fields before advertising availability",
      () =>
        Effect.gen(function* () {
          for (const reply of [
            { status: "bound", configuredReportId: "d".repeat(64) },
            { status: "bound", configuredReportId: reportId, verified: true },
            { status: "unconfigured", reason: "reader_failed", configuredReportId: reportId },
          ]) {
            const input = harness();
            const checked = yield* input.reader().checkBinding.pipe(Effect.forkChild);
            yield* Effect.promise(() => input.spawned);
            input.reply(reply);
            expect(yield* Fiber.join(checked)).toEqual({
              status: "unconfigured",
              reason: "host_binding_unverified",
              configuredReportId: reportId,
            });
            vi.restoreAllMocks();
          }
        }),
    );

    it.effect("preserves typed unconfigured check replies including a null configured report", () =>
      Effect.gen(function* () {
        const input = harness();
        const checked = yield* input.reader().checkBinding.pipe(Effect.forkChild);
        yield* Effect.promise(() => input.spawned);
        const reply = {
          status: "unconfigured",
          reason: "report_unconfigured",
          configuredReportId: null,
        };
        input.reply(reply);
        expect(yield* Fiber.join(checked)).toEqual(reply);
      }),
    );

    it.effect("reverifies pins for each read and returns the direct closed projection text", () =>
      Effect.gen(function* () {
        const input = harness();
        const reader = input.reader();
        const response = yield* read(input).pipe(Effect.forkChild);
        yield* Effect.promise(() => input.spawned);
        input.reply(report());
        expect(yield* Fiber.join(response)).toBe(encodeJson(report()));
        expect(input.runtime.spawn).toHaveBeenCalledWith(
          binding.python.path,
          expect.arrayContaining(["read", "--report-id", reportId]),
          expect.anything(),
        );
        const spawnedAgain = input.nextSpawn();
        const next = yield* reader
          .readSummary({ reportId, limits: TOKEN_ACCOUNTING_READER_LIMITS })
          .pipe(Effect.forkChild);
        yield* Effect.promise(() => spawnedAgain);
        input.reply(report());
        expect(yield* Fiber.join(next)).toBe(encodeJson(report()));
        expect(input.verify).toHaveBeenCalledTimes(2);
      }),
    );

    it.effect(
      "preserves closed unavailable statuses and rejects newer failure or projection fields",
      () =>
        Effect.gen(function* () {
          const cases = [
            [
              { status: "missing", reason: "configured_report_missing" },
              { status: "missing", reason: "configured_report_missing" },
            ],
            [
              { status: "oversized", reason: "input_too_large" },
              { status: "oversized", reason: "input_too_large" },
            ],
            [
              { status: "unsupported", reason: "report_schema_unsupported" },
              { status: "unsupported", reason: "report_schema_unsupported" },
            ],
            [
              { status: "invalid", reason: "report_identity_mismatch" },
              { status: "invalid", reason: "report_identity_mismatch" },
            ],
            [
              { status: "missing", reason: "configured_report_missing", path: "/private" },
              { status: "invalid", reason: "projection_invalid" },
            ],
            [
              { ...report(), report_id: "d".repeat(64) },
              { status: "invalid", reason: "configured_report_id_mismatch" },
            ],
            [
              { ...report(), future_metadata: {} },
              { status: "invalid", reason: "projection_invalid" },
            ],
          ];
          for (const [reply, expected] of cases) {
            const input = harness();
            const response = yield* read(input).pipe(Effect.forkChild);
            yield* Effect.promise(() => input.spawned);
            input.reply(reply);
            expect(yield* Fiber.join(response)).toEqual(expected);
            expect(input.listeners()).toBe(0);
            vi.restoreAllMocks();
          }
        }),
    );

    it.effect("rejects invalid UTF-8 and malformed JSON without diagnostic text", () =>
      Effect.gen(function* () {
        for (const bytes of [
          new Uint8Array([0xff]),
          new TextEncoder().encode("invalid JSON /private"),
        ]) {
          const input = harness();
          const response = yield* read(input).pipe(Effect.forkChild);
          yield* Effect.promise(() => input.spawned);
          input.stdoutBytes(bytes);
          input.close();
          expect(yield* Fiber.join(response)).toEqual({
            status: "invalid",
            reason: "projection_invalid",
          });
          expect(input.listeners()).toBe(0);
          vi.restoreAllMocks();
        }
      }),
    );

    it.effect("caps raw stdout chunks before concatenation and kills only the captured child", () =>
      Effect.gen(function* () {
        const input = harness();
        const response = yield* read(input).pipe(Effect.forkChild);
        yield* Effect.promise(() => input.spawned);
        input.stdoutBytes(new Uint8Array(TOKEN_ACCOUNTING_MAX_RESPONSE_BYTES));
        expect(input.child.kill).not.toHaveBeenCalled();
        input.stdoutBytes(new Uint8Array(1));
        expect(yield* Fiber.join(response)).toEqual({
          status: "oversized",
          reason: "projection_too_large",
        });
        expect(input.child.kill).toHaveBeenCalledTimes(1);
        expect(input.child.destroyOutputs).toHaveBeenCalledTimes(1);
        expect(input.listeners()).toBe(0);
      }),
    );

    it.effect("accepts the exact stdout cap and discards bounded stderr without returning it", () =>
      Effect.gen(function* () {
        const input = harness();
        const response = yield* read(input).pipe(Effect.forkChild);
        yield* Effect.promise(() => input.spawned);
        const text = encodeJson({ status: "missing", reason: "configured_report_missing" });
        input.stdout(text + " ".repeat(TOKEN_ACCOUNTING_MAX_RESPONSE_BYTES - text.length));
        input.stderr(new TextEncoder().encode("/synthetic/private/stderr"));
        input.close();
        expect(yield* Fiber.join(response)).toEqual({
          status: "missing",
          reason: "configured_report_missing",
        });
        expect(input.child.kill).not.toHaveBeenCalled();
        expect(input.listeners()).toBe(0);
      }),
    );

    it.effect("fails closed on excessive stderr, child errors, nonzero exit and spawn errors", () =>
      Effect.gen(function* () {
        for (const failure of ["stderr", "error", "exit", "spawn"] as const) {
          const input = harness();
          if (failure === "spawn")
            vi.mocked(input.runtime.spawn).mockImplementation(() => {
              throw new Error("/private executable");
            });
          const response = yield* read(input).pipe(Effect.forkChild);
          if (failure !== "spawn") {
            yield* Effect.promise(() => input.spawned);
            if (failure === "stderr") input.stderr(new Uint8Array(16 * 1024 + 1));
            if (failure === "error") input.error();
            if (failure === "exit") input.close(1);
          }
          expect(yield* Fiber.join(response)).toEqual({
            status: "reader_failed",
            reason: "reader_failed",
          });
          expect(input.child.kill).toHaveBeenCalledTimes(
            failure === "stderr" || failure === "error" ? 1 : 0,
          );
          expect(input.listeners()).toBe(0);
          vi.restoreAllMocks();
        }
      }),
    );

    it.effect(
      "includes pin verification in the deadline and never spawns after late verification",
      () =>
        Effect.gen(function* () {
          const input = harness();
          let resolve!: (value: Config.ProcessReaderBinding) => void;
          const pending = new Promise<Config.ProcessReaderBinding>((ready) => {
            resolve = ready;
          });
          input.verify.mockImplementation(() => pending);
          const response = yield* read(input).pipe(Effect.forkChild);
          yield* Effect.promise(() => input.scheduled);
          expect(input.runtime.deadline).toHaveBeenCalledTimes(1);
          input.expire();
          expect(yield* Fiber.join(response)).toEqual({
            status: "reader_failed",
            reason: "reader_timeout",
          });
          resolve(binding);
          yield* Effect.promise(() => pending);
          yield* Effect.yieldNow;
          expect(input.runtime.spawn).not.toHaveBeenCalled();
          expect(input.listeners()).toBe(0);
        }),
    );

    it.effect("kills and removes all waiters on a child deadline", () =>
      Effect.gen(function* () {
        const input = harness();
        const response = yield* read(input).pipe(Effect.forkChild);
        yield* Effect.promise(() => input.spawned);
        input.expire();
        expect(yield* Fiber.join(response)).toEqual({
          status: "reader_failed",
          reason: "reader_timeout",
        });
        expect(input.child.kill).toHaveBeenCalledTimes(1);
        expect(input.listeners()).toBe(0);
      }),
    );

    it.effect("cancels only its own captured child and cleans all callbacks on interruption", () =>
      Effect.gen(function* () {
        const input = harness();
        const fiber = yield* read(input).pipe(Effect.forkChild);
        yield* Effect.promise(() => input.spawned);
        yield* Fiber.interrupt(fiber);
        expect(input.child.kill).toHaveBeenCalledTimes(1);
        expect(input.child.destroyOutputs).toHaveBeenCalledTimes(1);
        expect(input.listeners()).toBe(0);
      }),
    );
  },
);
