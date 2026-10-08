// @effect-diagnostics nodeBuiltinImport:off
import { describe, expect, it } from "vite-plus/test";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NetAddress from "effect/unstable/net/NetAddress";
import {
  assertQualifiedTrialBinding,
  decodeQualifiedTrialReceipt,
  decodeQualifiedTrialGrant,
  makeQualifiedTrialReceipt,
  qualifiedResumeReservationPath,
  reserveQualifiedResume,
  sameQualifiedTrialIdentity,
  type QualifiedTrialReceipt,
} from "./qualifiedStartup.ts";
import type { StagedQualifiedRuntime } from "./qualifiedRuntime.ts";

const receipt: QualifiedTrialReceipt = {
  protocol: 4,
  startupGateProtocol: 1,
  updateId: "synthetic-update",
  stagedHandle: "11111111-1111-4111-8111-111111111111",
  home: "/synthetic",
  databasePath: "/synthetic/userdata/statev2.sqlite",
  serviceUserdata: "/synthetic/userdata",
  environmentId: "synthetic-environment",
  version: "0.0.0-preview.20261002.101.1",
  sourceSha: "a".repeat(40),
  sourceTree: "b".repeat(40),
  listener: { family: "IPv4", address: "127.0.0.1", port: 43123, scopeId: 0 },
  processId: 123,
  resumeHeld: true,
};

function staged(r: QualifiedTrialReceipt): StagedQualifiedRuntime {
  return {
    protocol: 1,
    stagedHandle: r.stagedHandle,
    binding: {
      baseDir: r.home,
      dbPath: r.databasePath,
      environmentId: r.environmentId,
      activeVersion: "0.0.0-preview.20261002.100",
      activeSourceSha: "c".repeat(40),
    },
    receipt: {
      protocol: 1,
      repository: "Jones-Systems/Jones-Code",
      channel: "jones-main",
      version: r.version,
      sourceSha: r.sourceSha,
      sourceTree: r.sourceTree,
      installedSourceSha: "c".repeat(40),
      runId: 101,
      runAttempt: 1,
      artifactId: 102,
      workflow: ".github/workflows/artifact-cli-linux.yml",
      artifactDigest: `sha256:${"d".repeat(64)}`,
      archiveSha256: "e".repeat(64),
      payloadSha256: "f".repeat(64),
      platform: "linux",
      architecture: "x64",
    },
  };
}

describe("qualified IPC proof", () => {
  it.each(Object.keys(receipt))("refuses missing receipt identity %s", (key) => {
    const incomplete = { ...receipt } as Record<string, unknown>;
    delete incomplete[key];
    expect(decodeQualifiedTrialReceipt(incomplete)).toBeUndefined();
  });

  it.each([
    { protocol: 3 },
    { startupGateProtocol: 2 },
    { updateId: "../other" },
    { stagedHandle: "latest" },
    { home: "relative" },
    { databasePath: "/synthetic/other.sqlite" },
    { serviceUserdata: "/other" },
    { sourceSha: "main" },
    { sourceTree: "tree" },
    { processId: 0 },
    { resumeHeld: false },
    { listener: { family: "IPv4", address: "::1", port: 123, scopeId: 0 } },
    { listener: { family: "IPv6", address: "::1", port: 123, scopeId: 2 } },
    { listener: { family: "IPv4", address: "127.0.0.1", port: 0, scopeId: 0 } },
  ])("refuses malformed proof %j", (patch) => {
    expect(decodeQualifiedTrialReceipt({ ...receipt, ...patch })).toBeUndefined();
  });

  it("requires exact generation and every receipt field in the grant", () => {
    expect(decodeQualifiedTrialGrant({ ...receipt, generation: receipt.updateId })).toEqual({
      ...receipt,
      generation: receipt.updateId,
    });
    expect(decodeQualifiedTrialGrant(receipt)).toBeUndefined();
    expect(decodeQualifiedTrialGrant({ ...receipt, generation: "other" })).toBeUndefined();
    for (const [key, value] of Object.entries(receipt)) {
      const altered = {
        ...receipt,
        [key]: typeof value === "string" ? `${value}-other` : undefined,
      };
      expect(sameQualifiedTrialIdentity(receipt, altered as QualifiedTrialReceipt)).toBe(false);
    }
  });

  it.each([
    "updateId",
    "stagedHandle",
    "environmentId",
    "version",
    "sourceSha",
    "sourceTree",
  ] as const)("refuses mismatch against retained candidate: %s", (key) => {
    const mismatch = {
      ...receipt,
      [key]: key === "sourceSha" || key === "sourceTree" ? "d".repeat(40) : `${receipt[key]}-other`,
    };
    expect(() =>
      assertQualifiedTrialBinding({
        updateId: receipt.updateId,
        qualified: staged(receipt),
        receipt: mismatch,
      }),
    ).toThrow();
  });
});

async function ownedRoot(body: (r: QualifiedTrialReceipt) => Promise<void>) {
  const scratch = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-qualified-startup-"));
  try {
    const home = await NodeFSP.realpath(scratch);
    const r: QualifiedTrialReceipt = {
      ...receipt,
      home,
      serviceUserdata: NodePath.join(home, "userdata"),
      databasePath: NodePath.join(home, "userdata", "statev2.sqlite"),
      processId: process.pid,
    };
    await NodeFSP.mkdir(r.serviceUserdata);
    await NodeFSP.writeFile(r.databasePath, "synthetic-database");
    await NodeFSP.mkdir(NodePath.dirname(qualifiedResumeReservationPath(r)), {
      recursive: true,
      mode: 0o700,
    });
    await body(r);
  } finally {
    await NodeFSP.rm(scratch, { recursive: true, force: true });
  }
}

describe("qualified IPC reservation", () => {
  it("binds the actual observed socket and runtime then publishes exclusively before resume", async () => {
    await ownedRoot(async (r) => {
      const observed = await makeQualifiedTrialReceipt({
        updateId: r.updateId,
        qualified: staged(r),
        witness: {
          home: r.home,
          databasePath: r.databasePath,
          serviceUserdata: r.serviceUserdata,
          environmentId: r.environmentId,
          version: r.version,
          processId: process.pid,
          buildMetadata: {
            jonesSource: {
              repository: "Jones-Systems/Jones-Code",
              sha: r.sourceSha,
              tree: r.sourceTree,
            },
          },
          listener: NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", 43123),
        },
      });
      expect(observed).toEqual(r);
      await reserveQualifiedResume({ receipt: observed });
      const target = qualifiedResumeReservationPath(r);
      expect(JSON.parse(await NodeFSP.readFile(target, "utf8"))).toEqual(r);
      await expect(reserveQualifiedResume({ receipt: r })).rejects.toThrow("already reserved");
      expect(await NodeFSP.readdir(NodePath.dirname(target))).toEqual(["resume-dispatched.json"]);
    });
  });

  it.each(["file-sync", "link", "directory-sync"] as const)(
    "retains uncertainty and cleans only captured scratch after %s failure",
    async (phase) => {
      await ownedRoot(async (r) => {
        const target = qualifiedResumeReservationPath(r);
        await expect(
          reserveQualifiedResume({
            receipt: r,
            adapter: {
              before: async (at) => {
                if (at === phase) throw new Error("synthetic durability failure");
              },
            },
          }),
        ).rejects.toThrow();
        expect(await NodeFSP.readdir(NodePath.dirname(target))).toEqual(
          phase === "directory-sync" ? ["resume-dispatched.json"] : [],
        );
        if (phase === "directory-sync") {
          expect(JSON.parse(await NodeFSP.readFile(target, "utf8"))).toEqual(r);
          await expect(reserveQualifiedResume({ receipt: r })).rejects.toThrow("already reserved");
        }
      });
    },
  );

  it.each(["link", "directory-sync"] as const)(
    "drains cancellation at %s and preserves a linked reservation",
    async (phase) => {
      await ownedRoot(async (r) => {
        const controller = new AbortController();
        await expect(
          reserveQualifiedResume({
            receipt: r,
            signal: controller.signal,
            adapter: {
              before: async (at) => {
                if (at === phase) controller.abort();
              },
            },
          }),
        ).rejects.toThrow();
        expect(await NodeFSP.readdir(NodePath.dirname(qualifiedResumeReservationPath(r)))).toEqual(
          phase === "directory-sync" ? ["resume-dispatched.json"] : [],
        );
      });
    },
  );

  it("rejects a symlink transaction root without altering its target", async () => {
    await ownedRoot(async (r) => {
      const transaction = NodePath.dirname(qualifiedResumeReservationPath(r));
      const retained = `${transaction}-retained`;
      await NodeFSP.rename(transaction, retained);
      await NodeFSP.symlink(retained, transaction);
      await expect(reserveQualifiedResume({ receipt: r })).rejects.toThrow("ownership");
      expect(await NodeFSP.readdir(retained)).toEqual([]);
    });
  });
});
