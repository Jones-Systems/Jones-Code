// @effect-diagnostics nodeBuiltinImport:off
import * as NodeChildProcess from "node:child_process";
import * as NodeSqlite from "node:sqlite";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { describe, expect, it } from "vite-plus/test";

import { validateNativeStoreAuthorityPath } from "./nativeStoreAuthorityPath.ts";
import { SERVICE_LAUNCHER_PROTOCOL } from "../cloud/serviceProtocol.ts";
import {
  advanceNativeStoreAuthority,
  decodeNativeStoreAuthorityState,
  fenceNativeStoreAuthority,
  initializeNativeStoreAuthority,
  initializeNativeStoreAuthorityForBaseDir,
  nativeStoreAuthorityPaths,
  readNativeStoreAuthorityState,
} from "./nativeStoreAuthorityPersistence.ts";

const withDirectory = (run: (directory: string) => void): void => {
  const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-native-authority-test-"));
  try {
    run(NodePath.join(root, "authority"));
  } finally {
    NodeFS.rmSync(root, { recursive: true, force: true });
  }
};

describe("native store authority persistence", () => {
  it("enrolls only from the persisted T3 environment identity", () => {
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-native-enroll-test-"));
    try {
      NodeFS.mkdirSync(NodePath.join(root, "userdata"), { recursive: true });
      NodeFS.writeFileSync(
        NodePath.join(root, "userdata", "environment-id"),
        "environment-native-enrollment\n",
      );
      NodeFS.mkdirSync(NodePath.join(root, "runtime"), { recursive: true });
      NodeFS.writeFileSync(
        NodePath.join(root, "runtime", "service-state.json"),
        JSON.stringify({ protocol: SERVICE_LAUNCHER_PROTOCOL, activeVersion: "1.0.0" }),
        { mode: 0o600 },
      );
      const state = initializeNativeStoreAuthorityForBaseDir(root, SERVICE_LAUNCHER_PROTOCOL);
      expect(state.environment_id).toBe("environment-native-enrollment");
      expect(state.state).toBe("active");
      expect(() =>
        initializeNativeStoreAuthorityForBaseDir(root, SERVICE_LAUNCHER_PROTOCOL),
      ).not.toThrow();
    } finally {
      NodeFS.rmSync(root, { recursive: true, force: true });
    }
  });

  it("rejects authority directories overlapping state, backups, and symlink aliases", () => {
    withDirectory((directory) => {
      const root = NodePath.dirname(directory);
      NodeFS.mkdirSync(NodePath.join(root, "userdata"));
      NodeFS.symlinkSync(NodePath.join(root, "userdata"), NodePath.join(root, "alias"));
      for (const target of [
        root,
        NodePath.join(root, "userdata", "authority"),
        NodePath.join(root, "alias", "missing"),
        NodePath.join(root, "runtime", "db-backup", "authority"),
      ]) {
        expect(() => validateNativeStoreAuthorityPath(root, target)).toThrow("overlaps");
      }
      expect(() => validateNativeStoreAuthorityPath(root, directory)).not.toThrow();
    });
  });

  it("creates one private authority and advances only after a durable fence", () => {
    withDirectory((authorityStateDir) => {
      const environmentId = "environment-native-authority";
      const initial = initializeNativeStoreAuthority(authorityStateDir, environmentId);
      expect(initial.authority_namespace).toMatch(/^t3-native:[0-9a-f-]{36}$/);
      expect(initial.store_generation).toBe(1);
      expect(initial.state).toBe("active");
      expect(NodeFS.statSync(authorityStateDir).mode & 0o777).toBe(0o700);
      expect(
        NodeFS.statSync(nativeStoreAuthorityPaths(authorityStateDir).statePath).mode & 0o777,
      ).toBe(0o600);

      const restarted = initializeNativeStoreAuthority(authorityStateDir, environmentId);
      expect(restarted).toEqual(initial);

      const fenced = fenceNativeStoreAuthority(authorityStateDir, environmentId);
      expect(fenced.state).toBe("fenced");
      expect(fenced.transition_id).toMatch(/^[0-9a-f-]{36}$/);
      expect(() => readNativeStoreAuthorityState(authorityStateDir)).not.toThrow();

      const databasePath = NodePath.join(NodePath.dirname(authorityStateDir), "state.sqlite");
      NodeFS.writeFileSync(databasePath, "SQLite format 3\0");
      const active = advanceNativeStoreAuthority(authorityStateDir, environmentId, databasePath);
      expect(active.state).toBe("active");
      expect(active.transition_id).toBeNull();
      expect(active.store_generation).toBe(2);
      expect(readNativeStoreAuthorityState(authorityStateDir)).toEqual(active);
    });
  });

  it("excludes a live writer across processes without unlinking the lock database", () => {
    withDirectory((authorityStateDir) => {
      initializeNativeStoreAuthority(authorityStateDir, "env-live-lock");
      const lockDatabasePath = NodePath.join(
        authorityStateDir,
        "native-store-authority-v1.lock.sqlite",
      );
      expect(NodeFS.statSync(lockDatabasePath).mode & 0o777).toBe(0o600);
      const lockDatabase = new NodeSqlite.DatabaseSync(lockDatabasePath);
      try {
        lockDatabase.exec("BEGIN EXCLUSIVE");
        expect(() => fenceNativeStoreAuthority(authorityStateDir, "env-live-lock")).toThrow();
        const contender = NodeChildProcess.spawnSync(
          process.execPath,
          [
            "--input-type=module",
            "-e",
            `
            const authority = await import(process.argv[1]);
            try {
              authority.fenceNativeStoreAuthority(process.argv[2], "env-live-lock");
              process.exitCode = 2;
            } catch (error) {
              if (error.code !== "source_unavailable") throw error;
            }
          `,
            new URL("./nativeStoreAuthorityPersistence.ts", import.meta.url).href,
            authorityStateDir,
          ],
          { timeout: 10_000, encoding: "utf8" },
        );
        expect(contender.error).toBeUndefined();
        expect(contender.status, contender.stderr).toBe(0);
        expect(readNativeStoreAuthorityState(authorityStateDir).state).toBe("active");
        expect(NodeFS.existsSync(lockDatabasePath)).toBe(true);
      } finally {
        lockDatabase.close();
      }
      expect(fenceNativeStoreAuthority(authorityStateDir, "env-live-lock").state).toBe("fenced");
      expect(NodeFS.existsSync(lockDatabasePath)).toBe(true);
    });
  });

  it("rejects non-private lock anchors and sidecars before opening SQLite", () => {
    for (const suffix of ["", "-journal", "-wal", "-shm"]) {
      withDirectory((authorityStateDir) => {
        initializeNativeStoreAuthority(authorityStateDir, "env-private-lock");
        const lockPath = `${nativeStoreAuthorityPaths(authorityStateDir).lockDatabasePath}${suffix}`;
        NodeFS.writeFileSync(lockPath, "", { mode: 0o600 });
        NodeFS.chmodSync(lockPath, 0o644);
        expect(() => fenceNativeStoreAuthority(authorityStateDir, "env-private-lock")).toThrow(
          "not a private regular file",
        );
        expect(readNativeStoreAuthorityState(authorityStateDir).state).toBe("active");
      });
    }
    withDirectory((authorityStateDir) => {
      initializeNativeStoreAuthority(authorityStateDir, "env-private-lock");
      const lockPath = nativeStoreAuthorityPaths(authorityStateDir).lockDatabasePath;
      NodeFS.renameSync(lockPath, `${lockPath}.saved`);
      NodeFS.symlinkSync(`${lockPath}.saved`, lockPath);
      expect(() => fenceNativeStoreAuthority(authorityStateDir, "env-private-lock")).toThrow(
        "not a private regular file",
      );
    });
  });

  it("does not reclaim legacy lock files whose owner cannot be proved absent", () => {
    withDirectory((authorityStateDir) => {
      initializeNativeStoreAuthority(authorityStateDir, "env-legacy-lock");
      const lockPath = nativeStoreAuthorityPaths(authorityStateDir).lockPath;
      NodeFS.writeFileSync(lockPath, "", { mode: 0o600 });
      expect(() => fenceNativeStoreAuthority(authorityStateDir, "env-legacy-lock")).toThrow();
      expect(NodeFS.readFileSync(lockPath, "utf8")).toBe("");
      expect(readNativeStoreAuthorityState(authorityStateDir).state).toBe("active");
    });
  });

  it("fails closed for malformed, fenced, mismatched, and unreviewable transitions", () => {
    expect(() =>
      decodeNativeStoreAuthorityState({
        record_version: "t3-native-store-authority/1.0.0",
        environment_id: "env",
        authority_namespace: "t3-native:00000000-0000-4000-8000-000000000000",
        store_generation: 1,
        state: "active",
        transition_id: "not-null",
      }),
    ).toThrow();

    withDirectory((authorityStateDir) => {
      initializeNativeStoreAuthority(authorityStateDir, "env-a");
      expect(() => fenceNativeStoreAuthority(authorityStateDir, "env-b")).toThrow(
        "Native authority environment changed",
      );
      const fenced = fenceNativeStoreAuthority(authorityStateDir, "env-a");
      expect(fenceNativeStoreAuthority(authorityStateDir, "env-a")).toEqual(fenced);
      expect(() =>
        advanceNativeStoreAuthority(
          authorityStateDir,
          "env-a",
          NodePath.join(authorityStateDir, "missing.sqlite"),
        ),
      ).toThrow();
      const invalidDatabasePath = NodePath.join(authorityStateDir, "invalid.sqlite");
      NodeFS.writeFileSync(invalidDatabasePath, "not sqlite");
      expect(() =>
        advanceNativeStoreAuthority(authorityStateDir, "env-a", invalidDatabasePath),
      ).toThrow("not a SQLite database");
    });
  });
});
