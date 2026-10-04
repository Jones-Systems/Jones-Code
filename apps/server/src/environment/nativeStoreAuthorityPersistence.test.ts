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
  advanceNativeStoreAuthorityForBaseDir,
  decodeNativeStoreAuthorityState,
  fenceNativeStoreAuthority,
  fenceNativeStoreAuthorityForBaseDir,
  initializeNativeStoreAuthority,
  initializeNativeStoreAuthorityForBaseDir,
  nativeStoreAuthorityPaths,
  readNativeStoreAuthorityState,
  readExistingNativeStoreAuthorityState,
  requireNativeStoreAuthorityLauncherProtocolForBaseDir,
  requireNativeStoreAuthoritySelectedStoreForBaseDir,
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
  it("existing-state reader never creates missing directories, state or writer locks", () => {
    withDirectory((authorityStateDir) => {
      const root = NodePath.dirname(authorityStateDir);
      expect(() => readExistingNativeStoreAuthorityState(authorityStateDir)).toThrow("missing");
      expect(NodeFS.readdirSync(root)).toEqual([]);
      NodeFS.mkdirSync(authorityStateDir, { mode: 0o700 });
      expect(() => readExistingNativeStoreAuthorityState(authorityStateDir)).toThrow("missing");
      expect(NodeFS.readdirSync(authorityStateDir)).toEqual([]);
      const state = {
        record_version: "t3-native-store-authority/1.0.0",
        environment_id: "synthetic-environment",
        authority_namespace: "t3-native:12345678-1234-4234-8234-123456789abc",
        store_generation: 7,
        state: "active",
        transition_id: null,
      };
      const statePath = nativeStoreAuthorityPaths(authorityStateDir).statePath;
      NodeFS.writeFileSync(statePath, JSON.stringify(state), { mode: 0o600 });
      const before = NodeFS.statSync(statePath);
      expect(readExistingNativeStoreAuthorityState(authorityStateDir)).toEqual(state);
      expect(NodeFS.readdirSync(authorityStateDir)).toEqual([NodePath.basename(statePath)]);
      expect(NodeFS.statSync(statePath).mtimeMs).toBe(before.mtimeMs);
      NodeFS.writeFileSync(statePath, "x".repeat(8193));
      expect(() => readExistingNativeStoreAuthorityState(authorityStateDir)).toThrow("bounded");
    });
  });
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
      const dbPath = NodePath.join(root, "userdata", "state.sqlite");
      const state = initializeNativeStoreAuthorityForBaseDir(
        root,
        SERVICE_LAUNCHER_PROTOCOL,
        dbPath,
      );
      expect(state.environment_id).toBe("environment-native-enrollment");
      expect(state.state).toBe("active");
      expect(() =>
        initializeNativeStoreAuthorityForBaseDir(root, SERVICE_LAUNCHER_PROTOCOL, dbPath),
      ).not.toThrow();
    } finally {
      NodeFS.rmSync(root, { recursive: true, force: true });
    }
  });

  it("rejects changed selected stores before enrollment, launcher trust or authority transitions", () => {
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-native-binding-test-"));
    try {
      const dbPath = NodePath.join(root, "userdata", "state.sqlite");
      const authorityDir = NodePath.join(root, "native-store-authority");
      NodeFS.mkdirSync(NodePath.dirname(dbPath));
      NodeFS.writeFileSync(dbPath, "SQLite format 3\0");
      NodeFS.writeFileSync(NodePath.join(root, "userdata", "environment-id"), "env-binding\n");
      const selectedPaths = [
        NodePath.join(root, "userdata", "statev2.sqlite"),
        NodePath.join(root, "custom.sqlite"),
      ];
      for (const selectedPath of selectedPaths) {
        NodeFS.copyFileSync(dbPath, selectedPath);
        expect(() =>
          initializeNativeStoreAuthorityForBaseDir(root, SERVICE_LAUNCHER_PROTOCOL, selectedPath),
        ).toThrow("separate native store qualification");
        expect(NodeFS.existsSync(authorityDir)).toBe(false);
      }
      const initial = initializeNativeStoreAuthority(authorityDir, "env-binding");
      const statePath = nativeStoreAuthorityPaths(authorityDir).statePath;
      const lockPath = nativeStoreAuthorityPaths(authorityDir).lockDatabasePath;
      for (const state of [initial, fenceNativeStoreAuthority(authorityDir, "env-binding")]) {
        NodeFS.writeFileSync(statePath, JSON.stringify(state), { mode: 0o600 });
        const stateBefore = NodeFS.readFileSync(statePath);
        const lockBefore = NodeFS.readFileSync(lockPath);
        for (const selectedPath of selectedPaths) {
          const selectedBefore = NodeFS.readFileSync(selectedPath);
          for (const operation of [
            () =>
              initializeNativeStoreAuthorityForBaseDir(
                root,
                SERVICE_LAUNCHER_PROTOCOL,
                selectedPath,
              ),
            () => fenceNativeStoreAuthorityForBaseDir(root, selectedPath),
            () => advanceNativeStoreAuthorityForBaseDir(root, selectedPath),
            () =>
              requireNativeStoreAuthorityLauncherProtocolForBaseDir(
                root,
                SERVICE_LAUNCHER_PROTOCOL,
                "1.0.0",
                selectedPath,
              ),
          ]) {
            expect(operation).toThrow("separate native store qualification");
            expect(NodeFS.readFileSync(statePath)).toEqual(stateBefore);
            expect(NodeFS.readFileSync(lockPath)).toEqual(lockBefore);
            expect(NodeFS.readFileSync(selectedPath)).toEqual(selectedBefore);
            expect(readExistingNativeStoreAuthorityState(authorityDir).store_generation).toBe(1);
          }
        }
      }
      for (const raw of [
        "corrupt authority record",
        JSON.stringify({ ...initial, environment_id: "another-environment", store_generation: 7 }),
        JSON.stringify({ ...initial, dbPath: selectedPaths[0] }),
      ]) {
        NodeFS.writeFileSync(statePath, raw);
        const lockBefore = NodeFS.readFileSync(lockPath);
        for (const selectedPath of selectedPaths) {
          expect(() => fenceNativeStoreAuthorityForBaseDir(root, selectedPath)).toThrow(
            "separate native store qualification",
          );
          expect(() => advanceNativeStoreAuthorityForBaseDir(root, selectedPath)).toThrow(
            "separate native store qualification",
          );
          expect(NodeFS.readFileSync(statePath, "utf8")).toBe(raw);
          expect(NodeFS.readFileSync(lockPath)).toEqual(lockBefore);
        }
      }
      NodeFS.unlinkSync(statePath);
      const missingEntries = NodeFS.readdirSync(authorityDir);
      for (const selectedPath of selectedPaths) {
        expect(() =>
          initializeNativeStoreAuthorityForBaseDir(root, SERVICE_LAUNCHER_PROTOCOL, selectedPath),
        ).toThrow("separate native store qualification");
        expect(fenceNativeStoreAuthorityForBaseDir(root, selectedPath)).toBeNull();
        expect(advanceNativeStoreAuthorityForBaseDir(root, selectedPath)).toBeNull();
        expect(NodeFS.readdirSync(authorityDir)).toEqual(missingEntries);
      }
      expect(() => requireNativeStoreAuthoritySelectedStoreForBaseDir(root, dbPath)).not.toThrow();
      NodeFS.unlinkSync(dbPath);
      NodeFS.symlinkSync(selectedPaths[0]!, dbPath);
      expect(() => requireNativeStoreAuthoritySelectedStoreForBaseDir(root, dbPath)).toThrow(
        "separate native store qualification",
      );
      expect(() => decodeNativeStoreAuthorityState({ ...initial, dbPath })).toThrow(
        "schema checks",
      );
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
