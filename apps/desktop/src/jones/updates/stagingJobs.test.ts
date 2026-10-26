// @effect-diagnostics nodeBuiltinImport:off - Synthetic staging roots under the user's cache.
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import {
  JONES_ACTIONS_RECEIPT_FILE,
  createJonesStageAttempt,
  publishJonesCompletedStage,
  type JonesStagedArtifact,
} from "@t3tools/shared/jones/jonesActions";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import { JonesBackgroundPreparation } from "./backgroundPreparation.ts";
import { stageJonesMacApp } from "./jonesMacStaging.ts";
import { reclaimJonesStagePayload } from "./stagingJobs.ts";

const faults = vi.hoisted(() => ({
  operation: "",
  path: "",
  gate: "",
  after: false,
  partial: false,
  foreignDevice: "",
  events: [] as string[],
}));

vi.mock("node:child_process", async (original) => ({
  ...(await original<typeof import("node:child_process")>()),
  execFile: vi.fn(),
}));
vi.mock("@t3tools/shared/jones/jonesActions", async (original) => ({
  ...(await original<typeof import("@t3tools/shared/jones/jonesActions")>()),
  // This suite proves cleanup and the next Mac attempt gate, not artifact qualification.
  validateJonesStagedArtifact: vi.fn(async () => undefined),
}));

vi.mock("node:fs/promises", async (original) => {
  const actual = await original<typeof import("node:fs/promises")>();
  const inject = async <T>(
    operation: string,
    path: unknown,
    action: () => Promise<T>,
  ): Promise<T> => {
    const name = String(path);
    faults.events.push(`${operation}:${name}`);
    const fail =
      faults.operation === operation &&
      faults.path === name &&
      (!faults.gate || faults.events.includes(faults.gate));
    if (fail && !faults.after) throw Object.assign(new Error("Injected EIO"), { code: "EIO" });
    const value = await action();
    if (fail) throw Object.assign(new Error("Injected EIO"), { code: "EIO" });
    return value;
  };
  return {
    ...actual,
    link: (...args: Parameters<typeof actual.link>) =>
      inject("link", args[1], () => actual.link(...args)),
    unlink: (...args: Parameters<typeof actual.unlink>) =>
      inject("unlink", args[0], () => actual.unlink(...args)),
    rmdir: (...args: Parameters<typeof actual.rmdir>) =>
      inject("rmdir", args[0], () => actual.rmdir(...args)),
    open: async (...args: Parameters<typeof actual.open>) => {
      const file = await actual.open(...args);
      const sync = file.sync.bind(file);
      file.sync = () => inject("sync", args[0], sync);
      return file;
    },
  };
});
vi.mock("node:fs", async (original) => {
  const actual = await original<typeof import("node:fs")>();
  const NodePath = await import("node:path");
  return {
    ...actual,
    promises: {
      ...actual.promises,
      lstat: async (...args: Parameters<typeof actual.promises.lstat>) => {
        const info = await actual.promises.lstat(...args);
        if (String(args[0]) === faults.foreignDevice)
          return Object.assign(info, { dev: Number(info.dev) + 1 });
        return info;
      },
      rm: async (...args: Parameters<typeof actual.promises.rm>) => {
        faults.events.push(`rm:${String(args[0])}`);
        if (faults.operation === "rm" && String(args[0]) === faults.path) {
          if (faults.partial) {
            // A pre-fix recursive attempt removal could already have lost completion.json.
            await actual.promises.unlink(
              NodePath.join(NodePath.dirname(String(args[0])), "completion.json"),
            );
            const names = await actual.promises.readdir(args[0]);
            if (names[0])
              await actual.promises.rm(NodePath.join(String(args[0]), names[0]), {
                recursive: true,
              });
          }
          if (faults.after) await actual.promises.rm(...args);
          throw Object.assign(new Error("Injected EIO"), { code: "EIO" });
        }
        return actual.promises.rm(...args);
      },
    },
  };
});

const resetFaults = () => {
  faults.operation = "";
  faults.path = "";
  faults.gate = "";
  faults.after = false;
  faults.partial = false;
  faults.foreignDevice = "";
  faults.events.length = 0;
};

const replaced = "a".repeat(64);
const current = "b".repeat(64);

const fixtures = new Set<string>();
afterEach(async () => {
  resetFaults();
  vi.mocked(NodeChildProcess.execFile).mockReset();
  for (const root of fixtures) {
    await NodeFSP.rm(root, { recursive: true, force: true });
    await expect(NodeFSP.lstat(root)).rejects.toMatchObject({ code: "ENOENT" });
    fixtures.delete(root);
  }
});

async function updatesRoot() {
  const parent = NodePath.join(NodeOS.homedir(), ".cache", "jones-updater-test-fixtures");
  await NodeFSP.mkdir(parent, { recursive: true, mode: 0o700 });
  const home = await NodeFSP.mkdtemp(NodePath.join(parent, "staging-jobs-"));
  fixtures.add(home);
  const root = NodePath.join(home, "jones-updates");
  await NodeFSP.mkdir(NodePath.join(root, "artifacts"), { recursive: true });
  await NodeFSP.mkdir(NodePath.join(root, "apps"), { recursive: true });
  return root;
}

/** A completed attempt in each root, the way the client and app staging publish them. */
async function completedStage(root: string, handle: string) {
  const artifact = await createJonesStageAttempt(NodePath.join(root, "artifacts"), handle);
  await NodeFSP.writeFile(NodePath.join(artifact.directory, "qualified.dmg"), "dmg-bytes");
  await NodeFSP.writeFile(NodePath.join(artifact.directory, JONES_ACTIONS_RECEIPT_FILE), "{}");
  await publishJonesCompletedStage(
    NodePath.join(root, "artifacts"),
    handle,
    artifact.directory,
    JONES_ACTIONS_RECEIPT_FILE,
  );
  const app = await createJonesStageAttempt(NodePath.join(root, "apps"), handle);
  const contents = NodePath.join(app.directory, "Candidate.app", "Contents", "Resources");
  await NodeFSP.mkdir(contents, { recursive: true });
  await NodeFSP.writeFile(NodePath.join(contents, "app.asar"), "asar-bytes");
  await NodeFSP.symlink("Resources", NodePath.join(contents, "..", "Link"));
  await NodeFSP.writeFile(NodePath.join(app.directory, "mac-app-receipt.json"), "{}");
  await publishJonesCompletedStage(
    NodePath.join(root, "apps"),
    handle,
    app.directory,
    "mac-app-receipt.json",
  );
  return { artifactAttempt: artifact.attemptRoot, appAttempt: app.attemptRoot };
}

const exists = (path: string) =>
  NodeFSP.lstat(path).then(
    () => true,
    () => false,
  );

describe("reclaimJonesStagePayload", () => {
  it("removes only the replaced handle's indexed attempts", async () => {
    const root = await updatesRoot();
    const old = await completedStage(root, replaced);
    const kept = await completedStage(root, current);
    await NodeFSP.mkdir(NodePath.join(root, "staging"));
    await NodeFSP.writeFile(
      NodePath.join(root, "staging", "selection.json"),
      JSON.stringify({ app: { handle: current } }),
    );

    const result = await reclaimJonesStagePayload(root, replaced);
    expect(result.status).toBe("reclaimed");
    // The payloads plus each attempt's linked completion record.
    expect(result.status === "reclaimed" && result.bytes).toBeGreaterThan(
      "dmg-bytes".length + "{}".length * 2 + "asar-bytes".length,
    );
    for (const path of [
      old.artifactAttempt,
      old.appAttempt,
      NodePath.join(root, "artifacts", "completed", `${replaced}.json`),
      NodePath.join(root, "apps", "completed", `${replaced}.json`),
    ])
      expect(await exists(path)).toBe(false);
    for (const path of [
      kept.artifactAttempt,
      kept.appAttempt,
      NodePath.join(root, "apps", "completed", `${current}.json`),
    ])
      expect(await exists(path)).toBe(true);
    expect(await reclaimJonesStagePayload(root, replaced)).toEqual({ status: "absent" });
  });

  it.each([
    ["a selection", "staging", "selection.json"],
    ["an attempt selection", "attempt-selections", "attempt.json"],
    ["a server runtime selection", "selections", "runtime.json"],
    ["a transaction record", "transactions", "txn-1/journal.json"],
    ["a retired preparation", "retired-preparations", "txn-2/prepare-intent.json"],
  ])("keeps a payload that %s still names", async (_label, directory, file) => {
    const root = await updatesRoot();
    const old = await completedStage(root, replaced);
    const record = NodePath.join(root, directory, file);
    await NodeFSP.mkdir(NodePath.dirname(record), { recursive: true });
    await NodeFSP.writeFile(record, JSON.stringify({ staged: { handle: replaced } }));

    expect(await reclaimJonesStagePayload(root, replaced)).toEqual({ status: "referenced" });
    expect(await exists(old.artifactAttempt)).toBe(true);
    expect(await exists(old.appAttempt)).toBe(true);
  });

  it("treats a transaction named for the handle as evidence", async () => {
    const root = await updatesRoot();
    await completedStage(root, replaced);
    await NodeFSP.mkdir(NodePath.join(root, "transactions", replaced), { recursive: true });
    expect(await reclaimJonesStagePayload(root, replaced)).toEqual({ status: "referenced" });
  });

  it("skips a transaction's recovery reserve but refuses an unbounded record", async () => {
    const root = await updatesRoot();
    await completedStage(root, replaced);
    const transaction = NodePath.join(root, "transactions", "txn-1");
    await NodeFSP.mkdir(transaction, { recursive: true });
    await NodeFSP.writeFile(
      NodePath.join(transaction, "reserve.bin"),
      Buffer.alloc(2 * 1024 * 1024),
    );
    expect(await reclaimJonesStagePayload(root, replaced)).toMatchObject({ status: "reclaimed" });

    await completedStage(root, current);
    await NodeFSP.mkdir(NodePath.join(root, "staging"), { recursive: true });
    await NodeFSP.writeFile(
      NodePath.join(root, "staging", "huge.json"),
      Buffer.alloc(2 * 1024 * 1024),
    );
    await expect(reclaimJonesStagePayload(root, current)).rejects.toThrow("bounded regular file");
  });

  it("refuses an invalid handle without reading any record", async () => {
    const root = await updatesRoot();
    await expect(reclaimJonesStagePayload(root, "../apps")).rejects.toThrow("Invalid");
  });

  it("holds both roots when a legacy directory has no receipt", async () => {
    const root = await updatesRoot();
    const legacy = NodePath.join(root, "apps", replaced);
    await NodeFSP.mkdir(NodePath.join(legacy, "Candidate.app"), { recursive: true });
    await NodeFSP.writeFile(NodePath.join(legacy, "mac-app-receipt.json"), "{}");
    const partial = NodePath.join(root, "artifacts", replaced);
    await NodeFSP.mkdir(partial);

    await expect(reclaimJonesStagePayload(root, replaced)).rejects.toMatchObject({
      code: "ENOENT",
    });
    expect(await exists(legacy)).toBe(true);
    expect(await exists(partial)).toBe(true);
  });
  it("reclaims a receipted exact-handle legacy stage", async () => {
    const root = await updatesRoot();
    const legacy = NodePath.join(root, "apps", replaced);
    await NodeFSP.mkdir(legacy);
    await NodeFSP.writeFile(NodePath.join(legacy, "mac-app-receipt.json"), "{}");
    expect(await reclaimJonesStagePayload(root, replaced)).toEqual({
      status: "reclaimed",
      bytes: 2,
    });
    expect(await exists(legacy)).toBe(false);
  });

  for (const kind of ["artifacts", "apps"] as const) {
    it.each([
      "marker-publish",
      "marker-sync",
      "index-unlink",
      "index-sync",
      "payload-before",
      "payload-partial",
      "payload-after",
      "completion-unlink",
      "attempt-rmdir",
      "parent-sync",
      "marker-retire",
    ])(
      `replays ${kind} %s interruption using durable external cleanup authority`,
      async (prefix) => {
        const root = await updatesRoot();
        const old = await completedStage(root, replaced);
        const kept = await completedStage(root, current);
        const attempt = kind === "apps" ? old.appAttempt : old.artifactAttempt;
        const marker = NodePath.join(root, "stage-cleanup", `${kind}-${replaced}.json`);
        const index = NodePath.join(root, kind, "completed", `${replaced}.json`);
        const operations: Record<string, [string, string, boolean]> = {
          "marker-publish": ["link", marker, true],
          "marker-sync": ["sync", NodePath.dirname(marker), false],
          "index-unlink": ["unlink", index, true],
          "index-sync": ["sync", NodePath.dirname(index), false],
          "payload-before": ["rm", NodePath.join(attempt, replaced), false],
          "payload-partial": ["rm", NodePath.join(attempt, replaced), false],
          "payload-after": ["rm", NodePath.join(attempt, replaced), true],
          "completion-unlink": ["unlink", NodePath.join(attempt, "completion.json"), true],
          "attempt-rmdir": ["rmdir", attempt, true],
          "parent-sync": ["sync", NodePath.dirname(attempt), false],
          "marker-retire": ["unlink", marker, true],
        };
        [faults.operation, faults.path, faults.after] = operations[prefix]!;
        faults.partial = prefix === "payload-partial";
        if (prefix === "marker-sync") faults.gate = `link:${marker}`;
        await expect(reclaimJonesStagePayload(root, replaced)).rejects.toMatchObject({
          code: "EIO",
        });
        if (!["marker-retire"].includes(prefix)) expect(await exists(marker)).toBe(true);
        if (prefix === "payload-partial")
          expect(await exists(NodePath.join(attempt, "completion.json"))).toBe(false);
        resetFaults();
        const result = await reclaimJonesStagePayload(root, replaced);
        expect(["absent", "reclaimed"]).toContain(result.status);
        expect(faults.events).toContain(`sync:${NodePath.join(root, "stage-cleanup")}`);
        if (["index-unlink", "index-sync"].includes(prefix))
          expect(faults.events).toContain(`sync:${NodePath.dirname(index)}`);
        if (["attempt-rmdir", "parent-sync"].includes(prefix))
          expect(faults.events).toContain(`sync:${NodePath.dirname(attempt)}`);
        for (const path of [old.artifactAttempt, old.appAttempt])
          expect(await exists(path)).toBe(false);
        expect(await exists(kept.artifactAttempt)).toBe(true);
        expect(await exists(kept.appAttempt)).toBe(true);
        expect(await NodeFSP.readdir(NodePath.join(root, "stage-cleanup"))).toEqual([]);
        // The exact unindexed attempt that blocked subsequent Mac staging is now gone.
        const next = await createJonesStageAttempt(NodePath.join(root, kind), "c".repeat(64));
        expect(await exists(next.directory)).toBe(true);
      },
    );
  }

  it("recovers the legacy index-unlinked prefix only from a surviving self-describing completion", async () => {
    const root = await updatesRoot();
    const old = await completedStage(root, replaced);
    for (const kind of ["artifacts", "apps"])
      await NodeFSP.unlink(NodePath.join(root, kind, "completed", `${replaced}.json`));
    expect(await reclaimJonesStagePayload(root, replaced)).toMatchObject({ status: "reclaimed" });
    expect(await exists(old.artifactAttempt)).toBe(false);
    expect(await exists(old.appAttempt)).toBe(false);
  });

  it.each([
    "mount",
    "payload-mount",
    "copier.json",
    "payload-copier",
    "corrupt-completion",
    "missing-completion",
    "symlink",
    "root-symlink",
    "cross-device",
    "unexplained-root",
  ])(
    "holds %s without changing unrelated paths or deleting activation evidence",
    async (condition) => {
      const root = await updatesRoot();
      const old = await completedStage(root, replaced);
      const kept = await completedStage(root, current);
      const receipt = NodePath.join(old.artifactAttempt, "completion.json");
      const payload = NodePath.join(old.artifactAttempt, replaced);
      if (condition === "mount") await NodeFSP.mkdir(NodePath.join(old.artifactAttempt, "mount"));
      if (condition === "payload-mount") await NodeFSP.mkdir(NodePath.join(payload, "mount"));
      if (condition === "payload-copier")
        await NodeFSP.writeFile(NodePath.join(payload, "copier.json"), "{}");
      if (condition === "copier.json")
        await NodeFSP.writeFile(NodePath.join(old.artifactAttempt, "copier.json"), "{}");
      if (condition === "corrupt-completion") await NodeFSP.writeFile(receipt, "{}");
      if (condition === "missing-completion") await NodeFSP.unlink(receipt);
      if (condition === "symlink")
        await NodeFSP.symlink(kept.artifactAttempt, NodePath.join(payload, "escape"));
      if (condition === "root-symlink") {
        const directory = NodePath.join(root, "artifacts");
        await NodeFSP.rename(directory, `${directory}-kept`);
        await NodeFSP.symlink(`${directory}-kept`, directory);
      }
      if (condition === "cross-device")
        faults.foreignDevice = NodePath.join(payload, "qualified.dmg");
      if (condition === "unexplained-root")
        await NodeFSP.mkdir(NodePath.join(root, "artifacts", "unknown"));
      const keptReceipt = await NodeFSP.readFile(
        NodePath.join(kept.artifactAttempt, "completion.json"),
        "utf8",
      );
      await expect(reclaimJonesStagePayload(root, replaced)).rejects.toThrow();
      expect(await exists(old.artifactAttempt)).toBe(true);
      expect(await exists(old.appAttempt)).toBe(true);
      expect(await exists(NodePath.join(root, "artifacts", "completed", `${replaced}.json`))).toBe(
        true,
      );
      expect(
        await NodeFSP.readFile(NodePath.join(kept.artifactAttempt, "completion.json"), "utf8"),
      ).toBe(keptReceipt);
    },
  );

  it.each([
    "corrupt-marker",
    "new-index",
    "new-unindexed-attempt",
    "changed-target",
    "new-reference",
  ])("keeps pinned partial cleanup held on %s", async (condition) => {
    const root = await updatesRoot();
    const old = await completedStage(root, replaced);
    faults.operation = "rm";
    faults.path = NodePath.join(old.artifactAttempt, replaced);
    await expect(reclaimJonesStagePayload(root, replaced)).rejects.toThrow("EIO");
    resetFaults();
    const marker = NodePath.join(root, "stage-cleanup", `artifacts-${replaced}.json`);
    if (condition === "corrupt-marker") await NodeFSP.writeFile(marker, "{}");
    if (condition === "new-index") {
      const next = await createJonesStageAttempt(NodePath.join(root, "artifacts"), replaced);
      await NodeFSP.writeFile(NodePath.join(next.directory, JONES_ACTIONS_RECEIPT_FILE), "{}");
      await publishJonesCompletedStage(
        NodePath.join(root, "artifacts"),
        replaced,
        next.directory,
        JONES_ACTIONS_RECEIPT_FILE,
      );
    }
    if (condition === "new-unindexed-attempt") {
      const next = await createJonesStageAttempt(NodePath.join(root, "artifacts"), replaced);
      await NodeFSP.writeFile(
        NodePath.join(next.attemptRoot, "completion.json"),
        JSON.stringify({ schema: 1, handle: replaced, directory: next.directory }),
      );
      await NodeFSP.writeFile(NodePath.join(next.directory, JONES_ACTIONS_RECEIPT_FILE), "{}");
    }
    if (condition === "changed-target") {
      await NodeFSP.rename(old.artifactAttempt, `${old.artifactAttempt}-kept`);
      await NodeFSP.mkdir(old.artifactAttempt);
    }
    if (condition === "new-reference") {
      await NodeFSP.mkdir(NodePath.join(root, "selections"));
      await NodeFSP.writeFile(
        NodePath.join(root, "selections", "live.json"),
        JSON.stringify({ handle: replaced }),
      );
      expect(await reclaimJonesStagePayload(root, replaced)).toEqual({ status: "referenced" });
    } else await expect(reclaimJonesStagePayload(root, replaced)).rejects.toThrow();
    expect(await exists(marker)).toBe(true);
    expect(await exists(old.artifactAttempt)).toBe(true);
    expect(await exists(old.appAttempt)).toBe(true);
  });

  it("unblocks the real Mac staging gate after replay of index-unlink EIO", async () => {
    const root = await updatesRoot();
    const old = await completedStage(root, replaced);
    const next = {
      candidate: { platform: "darwin" },
      payloadPath: NodePath.join(root, "fixture.dmg"),
      stagedHandle: "c".repeat(64),
    } as JonesStagedArtifact;
    faults.operation = "rm";
    faults.path = NodePath.join(old.appAttempt, replaced);
    await expect(reclaimJonesStagePayload(root, replaced)).rejects.toThrow("EIO");
    await expect(stageJonesMacApp(next, NodePath.join(root, "apps"), "darwin")).rejects.toThrow(
      "incomplete or unknown",
    );
    expect(NodeChildProcess.execFile).not.toHaveBeenCalled();
    resetFaults();
    await reclaimJonesStagePayload(root, replaced);
    vi.mocked(NodeChildProcess.execFile).mockImplementation(
      (_command, _args, _options, callback) => {
        if (typeof callback !== "function") throw new Error("Missing native callback.");
        callback(new Error("Synthetic native boundary"), "", "");
        return { exitCode: 1, signalCode: null } as ReturnType<typeof NodeChildProcess.execFile>;
      },
    );
    await expect(stageJonesMacApp(next, NodePath.join(root, "apps"), "darwin")).rejects.toThrow();
    expect(NodeChildProcess.execFile).toHaveBeenCalledWith(
      "/usr/bin/hdiutil",
      expect.arrayContaining(["attach"]),
      expect.any(Object),
      expect.any(Function),
    );
  });

  it("keeps the background ledger through real EIO retries and clears it only after both roots finish", async () => {
    const root = await updatesRoot();
    const old = await completedStage(root, replaced);
    let record = { enabled: false, unreclaimed: [replaced] };
    let now = 0;
    const background = new JonesBackgroundPreparation({
      controller: {
        busy: null,
        latestAvailable: undefined,
        stagedBuild: undefined,
        preparationHold: undefined,
        check: async () => true,
        download: async () => ({ completed: false }),
        discard: async () => ({ accepted: false, completed: false }),
        supersede: async () => "refused",
        reclaimExclusive: async (reclaim) => {
          await reclaim();
          return true;
        },
        setPreparationStatus: async () => undefined,
      },
      store: {
        read: async () => record,
        write: async (next) => {
          record = { ...next, unreclaimed: [...next.unreclaimed] };
        },
      },
      now: () => now,
      reclaim: (handle) => reclaimJonesStagePayload(root, handle),
    });
    faults.operation = "rm";
    faults.path = NodePath.join(old.appAttempt, replaced);
    try {
      await background.start();
      await background.idle();
      expect(record.unreclaimed).toEqual([replaced]);
      now += 60 * 60_000;
      await background.tick();
      expect(record.unreclaimed).toEqual([replaced]);
      expect(await exists(old.appAttempt)).toBe(true);
      resetFaults();
      now += 60 * 60_000;
      await background.tick();
      expect(record.unreclaimed).toEqual([]);
      expect(await exists(old.artifactAttempt)).toBe(false);
      expect(await exists(old.appAttempt)).toBe(false);
    } finally {
      background.close();
    }
  });
});
