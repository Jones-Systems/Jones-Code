import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import { expect, it, vi } from "@effect/vitest";

import { makeProcessReader, type ProcessReaderRuntime } from "./ProcessReader.ts";
import type { ProcessReaderConfiguration, ProcessReaderStat } from "./ProcessReaderConfig.ts";
import {
  TOKEN_ACCOUNTING_READER_LIMITS,
  unconfiguredReader,
  type TokenAccountingReaderPort,
} from "./Reader.ts";
import { makeRuntimeReader } from "./RuntimeReader.ts";

const reportId = "a".repeat(64);
const bindingSha256 = "b".repeat(64);
const bindingPath = "/fixture/binding.json";
const environment = {
  T3_TOKEN_ACCOUNTING_BINDING_PATH: bindingPath,
  T3_TOKEN_ACCOUNTING_BINDING_SHA256: bindingSha256,
  T3_TOKEN_ACCOUNTING_REPORT_ID: reportId,
};
const request = { reportId, limits: TOKEN_ACCOUNTING_READER_LIMITS };
const unavailable = { status: "unconfigured", reason: "host_binding_unverified" } as const;

function runtime(): ProcessReaderRuntime {
  const stat = (directory: boolean): ProcessReaderStat => ({
    size: 0,
    uid: 42,
    mode: directory ? 0o40755 : 0o100600,
    nlink: 1,
    dev: 1,
    ino: 1,
    isFile: () => !directory,
    isDirectory: () => directory,
    isSymbolicLink: () => false,
  });
  return {
    fileSystem: {
      lstat: vi.fn(async (path: string) => stat(path !== bindingPath)),
      open: vi.fn().mockRejectedValue(new Error("synthetic descriptor unavailable")),
    },
    identity: vi.fn(async () => ({ realUid: 42, effectiveUid: 42, savedUid: 42 })),
    spawn: vi.fn(() => {
      throw new Error("unexpected subprocess");
    }),
    deadline: vi.fn(() => () => {}),
  };
}

it.layer(Layer.succeed(HostProcessPlatform, "linux"))("saved accounting startup reader", (it) => {
  it.effect("returns the existing unconfigured port when all three keys are absent", () =>
    Effect.gen(function* () {
      const factory = vi.fn(() => unconfiguredReader);
      const reader = makeRuntimeReader({}, factory);
      expect(reader).toBe(unconfiguredReader);
      expect(yield* reader.checkBinding).toEqual({
        status: "unconfigured",
        reason: "reader_unconfigured",
        configuredReportId: null,
      });
      expect(yield* reader.readSummary(request)).toEqual({
        status: "unconfigured",
        reason: "reader_unconfigured",
      });
      expect(factory).not.toHaveBeenCalled();
    }),
  );

  it.effect(
    "reads only the three startup keys and preserves their bytes in the injected constructor",
    () =>
      Effect.gen(function* () {
        const observed: PropertyKey[] = [];
        const allowed = new Set(Object.keys(environment));
        const input = new Proxy<Readonly<Record<string, string | undefined>>>(
          {
            ...environment,
            PATH: "/ignored",
            PYTHONPATH: "/ignored",
            T3_TOKEN_ACCOUNTING_ARCHIVE_PATH: "/ignored",
          },
          {
            get: (target, key) => {
              expect(typeof key === "string" && allowed.has(key)).toBe(true);
              observed.push(key);
              return target[key as string];
            },
            ownKeys: () => {
              throw new Error("environment must not be enumerated");
            },
          },
        );
        const missing = { status: "missing", reason: "configured_report_missing" } as const;
        const injected: TokenAccountingReaderPort = {
          checkBinding: Effect.succeed({ status: "bound", configuredReportId: reportId }),
          readSummary: () => Effect.succeed(missing),
        };
        const factory = vi.fn(() => injected);
        const reader = makeRuntimeReader(input, factory);
        expect(observed).toEqual(Object.keys(environment));
        expect(factory).toHaveBeenCalledExactlyOnceWith({ bindingPath, bindingSha256, reportId });
        expect(reader).toBe(injected);
        expect(yield* reader.checkBinding).toEqual({
          status: "bound",
          configuredReportId: reportId,
        });
        expect(yield* reader.readSummary(request)).toEqual(missing);
      }),
  );

  it.effect(
    "keeps every partial configuration unavailable without constructing a process reader",
    () =>
      Effect.gen(function* () {
        for (const input of [
          { T3_TOKEN_ACCOUNTING_BINDING_PATH: bindingPath },
          { T3_TOKEN_ACCOUNTING_BINDING_SHA256: bindingSha256 },
          { T3_TOKEN_ACCOUNTING_REPORT_ID: reportId },
          {
            T3_TOKEN_ACCOUNTING_BINDING_PATH: bindingPath,
            T3_TOKEN_ACCOUNTING_BINDING_SHA256: bindingSha256,
          },
          {
            T3_TOKEN_ACCOUNTING_BINDING_PATH: bindingPath,
            T3_TOKEN_ACCOUNTING_REPORT_ID: reportId,
          },
          {
            T3_TOKEN_ACCOUNTING_BINDING_SHA256: bindingSha256,
            T3_TOKEN_ACCOUNTING_REPORT_ID: reportId,
          },
        ]) {
          const factory = vi.fn(() => unconfiguredReader);
          const reader = makeRuntimeReader(input, factory);
          expect(yield* reader.checkBinding).toEqual({
            ...unavailable,
            configuredReportId: "T3_TOKEN_ACCOUNTING_REPORT_ID" in input ? reportId : null,
          });
          expect(yield* reader.readSummary(request)).toEqual(unavailable);
          expect(factory).not.toHaveBeenCalled();
        }
      }),
  );

  it.effect(
    "rejects malformed paths or pins without trimming, normalizing, throwing or opening anything",
    () =>
      Effect.gen(function* () {
        for (const input of [
          { ...environment, T3_TOKEN_ACCOUNTING_BINDING_PATH: "relative.json" },
          { ...environment, T3_TOKEN_ACCOUNTING_BINDING_PATH: "/fixture/../binding.json" },
          { ...environment, T3_TOKEN_ACCOUNTING_BINDING_PATH: "/fixture/binding.json\0" },
          { ...environment, T3_TOKEN_ACCOUNTING_BINDING_SHA256: "" },
          { ...environment, T3_TOKEN_ACCOUNTING_BINDING_SHA256: bindingSha256 + "\n" },
          { ...environment, T3_TOKEN_ACCOUNTING_BINDING_SHA256: "B".repeat(64) },
          { ...environment, T3_TOKEN_ACCOUNTING_REPORT_ID: "A".repeat(64) },
          { ...environment, T3_TOKEN_ACCOUNTING_REPORT_ID: reportId + "\n" },
          { ...environment, T3_TOKEN_ACCOUNTING_REPORT_ID: "not-a-report-id" },
          {
            T3_TOKEN_ACCOUNTING_BINDING_PATH: "",
            T3_TOKEN_ACCOUNTING_BINDING_SHA256: "",
            T3_TOKEN_ACCOUNTING_REPORT_ID: "",
          },
        ]) {
          const native = runtime();
          const factory = vi.fn((configuration: ProcessReaderConfiguration) =>
            makeProcessReader(configuration, native),
          );
          const reader = makeRuntimeReader(input, factory);
          expect(yield* reader.checkBinding).toEqual({
            ...unavailable,
            configuredReportId: input.T3_TOKEN_ACCOUNTING_REPORT_ID === reportId ? reportId : null,
          });
          expect(yield* reader.readSummary(request)).toEqual(unavailable);
          expect(factory).not.toHaveBeenCalled();
          expect(native.identity).not.toHaveBeenCalled();
          expect(native.fileSystem.lstat).not.toHaveBeenCalled();
          expect(native.fileSystem.open).not.toHaveBeenCalled();
          expect(native.spawn).not.toHaveBeenCalled();
          expect(native.deadline).not.toHaveBeenCalled();
        }
      }),
  );

  it.effect("constructs the configured process port with zero host or archive I/O", () =>
    Effect.sync(() => {
      const native = runtime();
      const factory = vi.fn((configuration: ProcessReaderConfiguration) =>
        makeProcessReader(configuration, native),
      );
      const reader = makeRuntimeReader(environment, factory);
      expect(factory).toHaveBeenCalledExactlyOnceWith({ bindingPath, bindingSha256, reportId });
      expect(reader).not.toBe(unconfiguredReader);
      expect(native.identity).not.toHaveBeenCalled();
      expect(native.fileSystem.lstat).not.toHaveBeenCalled();
      expect(native.fileSystem.open).not.toHaveBeenCalled();
      expect(native.spawn).not.toHaveBeenCalled();
      expect(native.deadline).not.toHaveBeenCalled();
    }),
  );

  it.effect("keeps caller report and path fields from selecting a different enrolled target", () =>
    Effect.gen(function* () {
      const native = runtime();
      const reader = makeRuntimeReader(environment, (configuration) =>
        makeProcessReader(configuration, native),
      );
      const callerPath = "/caller/report.json";
      expect(
        yield* reader.readSummary({
          ...request,
          reportId: "c".repeat(64),
          path: callerPath,
        } as never),
      ).toEqual({ status: "invalid", reason: "configured_report_id_mismatch" });
      expect(native.identity).not.toHaveBeenCalled();
      expect(native.fileSystem.open).not.toHaveBeenCalled();
      expect(yield* reader.readSummary({ ...request, path: callerPath } as never)).toEqual(
        unavailable,
      );
      expect(native.fileSystem.open).toHaveBeenCalledExactlyOnceWith(bindingPath);
      expect(native.spawn).not.toHaveBeenCalled();
    }),
  );

  it.effect(
    "keeps a failed constructor local without exposing its exception or breaking startup",
    () =>
      Effect.gen(function* () {
        const factory = vi.fn(() => {
          throw new Error("/synthetic/private/constructor");
        });
        const reader = makeRuntimeReader(environment, factory);
        expect(yield* reader.checkBinding).toEqual({
          ...unavailable,
          configuredReportId: reportId,
        });
        expect(yield* reader.readSummary(request)).toEqual(unavailable);
        expect(factory).toHaveBeenCalledTimes(1);
      }),
  );
});
