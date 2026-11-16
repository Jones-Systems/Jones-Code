// All filesystem entries below are in-memory; these checks never enroll or inspect a host.
// @effect-diagnostics nodeBuiltinImport:off
import * as NodeCrypto from "node:crypto";

import { describe, expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";

import {
  ProcessReaderBinding,
  ProcessReaderConfiguration,
  TOKEN_ACCOUNTING_SOURCE_PATHS,
  verifyProcessReaderConfiguration,
  type ProcessReaderFileSystem,
  type ProcessReaderStat,
} from "./ProcessReaderConfig.ts";

const uid = 42;
const reportId = "a".repeat(64);
const hash = (value: string) => NodeCrypto.createHash("sha256").update(value).digest("hex");
const encode = (value: string) => new TextEncoder().encode(value);
const stat = (directory = false, size = 0): ProcessReaderStat => ({
  size,
  uid,
  mode: directory ? 0o40755 : 0o100600,
  nlink: 1,
  dev: 1,
  ino: 1,
  isFile: () => !directory,
  isDirectory: () => directory,
  isSymbolicLink: () => false,
});

function fixture() {
  const sourceRoot = "/fixture/src";
  const files = new Map<string, Uint8Array>();
  const closure = Object.fromEntries(
    TOKEN_ACCOUNTING_SOURCE_PATHS.map((path) => {
      files.set(`${sourceRoot}/${path}`, encode(path));
      return [path, hash(path)];
    }),
  );
  files.set("/fixture/python", encode("pinned interpreter"));
  files.set("/etc/machine-id", encode(`${"b".repeat(32)}\n`));
  const binding = {
    schema: "programmatic-token-info.saved-accounting-binding/v1",
    machine_id_sha256: hash("b".repeat(32)),
    uid,
    python: { path: "/fixture/python", sha256: hash("pinned interpreter") },
    helper: {
      path: `${sourceRoot}/codex_v3/token_info/saved_reader.py`,
      sha256: closure["codex_v3/token_info/saved_reader.py"],
    },
    source_root: sourceRoot,
    source_closure: closure,
    source_closure_sha256: hash(JSON.stringify(closure)),
    archive_root: "/fixture/archive",
    report_id: reportId,
    authority_effect: "none",
  };
  const opened: string[] = [];
  const closed: string[] = [];
  const stats = new Map<string, ProcessReaderStat>();
  const fileSystem: ProcessReaderFileSystem = {
    lstat: async (path) => stats.get(path) ?? stat(!files.has(path), files.get(path)?.byteLength),
    open: async (path) => {
      opened.push(path);
      const bytes = files.get(path);
      if (bytes === undefined) throw new Error("synthetic missing file");
      return {
        stat: async () => stats.get(path) ?? stat(false, bytes.byteLength),
        read: async (buffer, position) => {
          const chunk = bytes.subarray(position, position + buffer.byteLength);
          buffer.set(chunk);
          return chunk.byteLength;
        },
        close: async () => {
          closed.push(path);
        },
      };
    },
  };
  const enroll = () => {
    const json = JSON.stringify(binding);
    files.set("/fixture/binding.json", encode(json));
    return { bindingPath: "/fixture/binding.json", bindingSha256: hash(json), reportId };
  };
  return {
    binding,
    files,
    stats,
    opened,
    closed,
    enroll,
    verify: () =>
      verifyProcessReaderConfiguration(
        enroll(),
        fileSystem,
        async () => ({ realUid: uid, effectiveUid: uid, savedUid: uid }),
        new AbortController().signal,
      ),
    fileSystem,
  };
}

describe("fixed process reader configuration", () => {
  it("rejects caller keys, non-normalized paths and malformed pins", () => {
    const decode = Schema.decodeUnknownSync(ProcessReaderConfiguration);
    const valid = fixture().enroll();
    expect(decode(valid)).toEqual(valid);
    for (const invalid of [
      { ...valid, extra: "ignored" },
      { ...valid, bindingPath: "relative" },
      { ...valid, bindingPath: "/fixture/../binding.json" },
      { ...valid, bindingPath: "/fixture//binding.json" },
      { ...valid, bindingPath: "/fixture/binding.json/" },
      { ...valid, bindingPath: "/fixture/binding.json\0" },
      { ...valid, reportId: "A".repeat(64) },
      { ...valid, bindingSha256: "not-a-hash" },
    ])
      expect(() => decode(invalid)).toThrow();
  });

  it("accepts only the closed fifteen-file binding closure", () => {
    const decode = Schema.decodeUnknownSync(ProcessReaderBinding);
    const { binding } = fixture();
    expect(decode(binding).source_closure).toEqual(binding.source_closure);
    const { [TOKEN_ACCOUNTING_SOURCE_PATHS[0]]: _first, ...missing } = binding.source_closure;
    for (const invalid of [
      { ...binding, metadata: "newer field" },
      { ...binding, python: { ...binding.python, flags: [] } },
      { ...binding, source_closure: missing },
      { ...binding, source_closure: { ...binding.source_closure, "other.py": reportId } },
      { ...binding, uid: 0 },
    ])
      expect(() => decode(invalid)).toThrow();
  });

  it("verifies all pins without opening the archive, report, index or raw sources", async () => {
    const input = fixture();
    const verified = await input.verify();
    expect(verified.report_id).toBe(reportId);
    expect(input.opened).toEqual([
      "/fixture/binding.json",
      "/etc/machine-id",
      "/fixture/python",
      ...TOKEN_ACCOUNTING_SOURCE_PATHS.map((path) => `/fixture/src/${path}`),
    ]);
    expect(input.closed).toEqual(input.opened);
  });

  it("rejects descriptor digest mismatch before reading interpreter or source pins", async () => {
    const input = fixture();
    const config = { ...input.enroll(), bindingSha256: "c".repeat(64) };
    await expect(
      verifyProcessReaderConfiguration(
        config,
        input.fileSystem,
        async () => ({ realUid: uid, effectiveUid: uid, savedUid: uid }),
        new AbortController().signal,
      ),
    ).rejects.toThrow();
    expect(input.opened).toEqual(["/fixture/binding.json"]);
    expect(input.closed).toEqual(input.opened);
  });

  it("rejects mismatched machine, UID, helper, closure and report bindings", async () => {
    for (const mutate of [
      (input: ReturnType<typeof fixture>) => {
        input.binding.machine_id_sha256 = reportId;
      },
      (input: ReturnType<typeof fixture>) => {
        input.binding.uid = uid + 1;
      },
      (input: ReturnType<typeof fixture>) => {
        input.binding.helper.path = "/fixture/foreign.py";
      },
      (input: ReturnType<typeof fixture>) => {
        input.binding.helper.sha256 = reportId;
      },
      (input: ReturnType<typeof fixture>) => {
        input.binding.source_closure_sha256 = reportId;
      },
      (input: ReturnType<typeof fixture>) => {
        input.binding.report_id = "d".repeat(64);
      },
    ]) {
      const input = fixture();
      mutate(input);
      await expect(input.verify()).rejects.toThrow();
      expect(input.opened).not.toContain("/fixture/python");
      expect(input.closed).toEqual(input.opened);
    }
  });

  it("rejects elevated, unequal and saved UID identities before any file opens", async () => {
    for (const identity of [
      { realUid: 0, effectiveUid: 0, savedUid: 0 },
      { realUid: uid, effectiveUid: uid + 1, savedUid: uid },
      { realUid: uid, effectiveUid: uid, savedUid: 0 },
    ]) {
      const input = fixture();
      await expect(
        verifyProcessReaderConfiguration(
          input.enroll(),
          input.fileSystem,
          async () => identity,
          new AbortController().signal,
        ),
      ).rejects.toThrow();
      expect(input.opened).toEqual([]);
    }
  });

  it("rejects symlink ancestors, writable custody, public bindings and multiple links", async () => {
    for (const [path, replacement] of [
      ["/fixture", { ...stat(true), isSymbolicLink: () => true }],
      ["/fixture", { ...stat(true), mode: 0o40777 }],
      ["/fixture/binding.json", { ...stat(), mode: 0o100640 }],
      ["/fixture/binding.json", { ...stat(), uid: 0 }],
      ["/fixture/binding.json", { ...stat(), nlink: 2 }],
    ] as const) {
      const input = fixture();
      input.stats.set(path, replacement);
      await expect(input.verify()).rejects.toThrow();
      expect(input.opened).toEqual([]);
    }
  });

  it("rejects changed interpreter and closure bytes and closes every opened descriptor", async () => {
    for (const path of [
      "/fixture/python",
      "/fixture/src/codex_v3/token_info/accounting_report.py",
    ]) {
      const input = fixture();
      input.files.set(path, encode("changed bytes"));
      await expect(input.verify()).rejects.toThrow();
      expect(input.closed).toEqual(input.opened);
      expect(input.opened).toContain(path);
    }
  });

  it("bounds the descriptor before parsing and rejects replacement after lstat", async () => {
    for (const kind of ["oversized", "replaced"] as const) {
      const input = fixture();
      const config = input.enroll();
      if (kind === "oversized") input.files.set(config.bindingPath, new Uint8Array(64 * 1024 + 1));
      const fs: ProcessReaderFileSystem =
        kind === "oversized"
          ? input.fileSystem
          : {
              ...input.fileSystem,
              open: async (path) => {
                const file = await input.fileSystem.open(path);
                return { ...file, stat: async () => ({ ...(await file.stat()), ino: 2 }) };
              },
            };
      await expect(
        verifyProcessReaderConfiguration(
          config,
          fs,
          async () => ({ realUid: uid, effectiveUid: uid, savedUid: uid }),
          new AbortController().signal,
        ),
      ).rejects.toThrow();
      expect(input.opened).toEqual([config.bindingPath]);
      expect(input.closed).toEqual(input.opened);
    }
  });

  it("closes a descriptor that resolves after cancellation without reading it", async () => {
    const input = fixture();
    const config = input.enroll();
    const abort = new AbortController();
    const fs: ProcessReaderFileSystem = {
      ...input.fileSystem,
      open: async (path) => {
        const file = await input.fileSystem.open(path);
        abort.abort();
        return file;
      },
    };
    await expect(
      verifyProcessReaderConfiguration(
        config,
        fs,
        async () => ({ realUid: uid, effectiveUid: uid, savedUid: uid }),
        abort.signal,
      ),
    ).rejects.toThrow();
    expect(input.opened).toEqual([config.bindingPath]);
    expect(input.closed).toEqual(input.opened);
  });
});
