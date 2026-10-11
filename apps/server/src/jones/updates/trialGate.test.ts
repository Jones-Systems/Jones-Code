// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFSP from "node:fs/promises";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeEvents from "node:events";
import { afterEach, expect, it, vi } from "vite-plus/test";
import * as NetAddress from "effect/net/NetAddress";
import { awaitJonesTrialCommit } from "./trialGate.ts";

vi.mock("node:fs/promises", async (importOriginal) => {
  const original = await importOriginal<typeof NodeFSP>();
  return {
    ...original,
    readFile: vi.fn(original.readFile),
    link: vi.fn(original.link),
    open: vi.fn(original.open),
  };
});
vi.mock("node:fs", async (importOriginal) => {
  const original = await importOriginal<typeof NodeFS>();
  return { ...original, watch: vi.fn(original.watch) };
});
afterEach(async () => {
  const original = await vi.importActual<typeof NodeFSP>("node:fs/promises");
  vi.mocked(NodeFSP.readFile).mockReset().mockImplementation(original.readFile);
  vi.mocked(NodeFSP.link).mockReset().mockImplementation(original.link);
  vi.mocked(NodeFSP.open).mockReset().mockImplementation(original.open);
  const originalFs = await vi.importActual<typeof NodeFS>("node:fs");
  vi.mocked(NodeFS.watch).mockReset().mockImplementation(originalFs.watch);
  vi.useRealTimers();
});

const listener = NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", 4888);

async function fixture<A>(body: (root: string) => Promise<A>): Promise<A> {
  const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-trial-test-"));
  try {
    await NodeFSP.mkdir(NodePath.join(root, "userdata"));
    await NodeFSP.mkdir(NodePath.join(root, "profile"));
    await NodeFSP.writeFile(NodePath.join(root, "userdata/statev2.sqlite"), "state");
    return await body(await NodeFSP.realpath(root));
  } finally {
    await NodeFSP.rm(root, { recursive: true, force: true });
    await expect(NodeFSP.lstat(root)).rejects.toMatchObject({ code: "ENOENT" });
  }
}
async function watchFile(file: string, signal: AbortSignal) {
  const watcher = NodeFS.watch(NodePath.dirname(file));
  let rejectWait: ((cause: Error) => void) | undefined;
  const aborted = () => rejectWait?.(new Error("Synthetic receipt observation cancelled."));
  let recheck: ReturnType<typeof setInterval> | undefined;
  try {
    await new Promise<void>((resolve, reject) => {
      rejectWait = reject;
      const inspect = () => {
        void NodeFSP.stat(file).then(
          () => resolve(),
          (error: NodeJS.ErrnoException) => {
            if (error.code !== "ENOENT") reject(error);
          },
        );
      };
      watcher.on("change", inspect);
      watcher.on("error", reject);
      // @effect-diagnostics-next-line globalTimers:off -- Native fs.watch test observer uses a timer to cover coalesced events and is always cleared in finally.
      recheck = setInterval(inspect, 250);
      signal.addEventListener("abort", aborted, { once: true });
      if (signal.aborted) aborted();
      else inspect();
    });
  } finally {
    clearInterval(recheck);
    signal.removeEventListener("abort", aborted);
    watcher.close();
  }
}

async function trial(root: string, transactionId = "synthetic-transaction") {
  const descriptor = {
    protocol: 1,
    startupGateProtocol: 1,
    transactionId,
    stagedHandle: "f".repeat(64),
    home: root,
    databasePath: NodePath.join(root, "userdata/statev2.sqlite"),
    profile: NodePath.join(root, "profile"),
    environmentId: "fixture",
    version: "0.0.44-preview.20261002.1",
    sourceSha: "a".repeat(40),
    sourceTree: "b".repeat(40),
    listener: "http://127.0.0.1:4888",
    trialReceiptPath: NodePath.join(root, "trial.json"),
    commitGrantPath: NodePath.join(root, "grant.json"),
  };
  const descriptorPath = NodePath.join(root, "descriptor.json");
  await NodeFSP.writeFile(descriptorPath, JSON.stringify(descriptor));
  return {
    ...descriptor,
    observedListener: listener,
    descriptorPath,
    buildMetadata: {
      jonesSource: {
        repository: "Jones-Systems/Jones-Code",
        sha: descriptor.sourceSha,
        tree: descriptor.sourceTree,
      },
    },
  };
}

async function committedTrial(root: string, legacy = false) {
  const initial = await trial(root, legacy ? "f".repeat(64) : undefined);
  const directory = NodePath.join(
    root,
    "runtime",
    "jones-updates",
    "transactions",
    initial.transactionId,
  );
  await NodeFSP.mkdir(directory, { recursive: true });
  const input = {
    ...initial,
    descriptorPath: NodePath.join(directory, "trial-descriptor.json"),
    trialReceiptPath: NodePath.join(directory, "trial-receipt.json"),
    commitGrantPath: NodePath.join(directory, "commit-grant.json"),
  };
  const manifestPath = NodePath.join(root, "runtime", "jones-active-install.json");
  const reservationPath = NodePath.join(directory, "resume-dispatched.json");
  const journalPath = NodePath.join(directory, "journal.json");
  const receipt = {
    ...input,
    resumeHeld: true,
    backendProcess: { pid: 123, identity: "original trial backend" },
  };
  const files = new Map<string, unknown>([
    [input.descriptorPath, input],
    [manifestPath, { ...input, owner: "desktop", generation: input.transactionId }],
    [input.commitGrantPath, { ...input, generation: input.transactionId }],
    [
      journalPath,
      {
        phase: "resumed",
        intent: {
          protocol: 1,
          transactionId: input.transactionId,
          staged: { handle: input.stagedHandle },
        },
      },
    ],
    [input.trialReceiptPath, receipt],
    [reservationPath, receipt],
  ]);
  for (const [path, value] of files) {
    const stored = { ...(value as Record<string, unknown>) };
    if (legacy) delete stored.stagedHandle;
    await NodeFSP.writeFile(path, JSON.stringify(stored));
  }
  return { input, files, manifestPath, journalPath, reservationPath };
}

it("admits repeated committed child restarts without replaying or replacing trial artifacts", async () =>
  fixture(async (root) => {
    const { input, files } = await committedTrial(root);
    const readArtifacts = () =>
      Promise.all([...files.keys()].map((path) => NodeFSP.readFile(path, "utf8")));
    const before = await readArtifacts();
    await awaitJonesTrialCommit(input);
    await awaitJonesTrialCommit(input);
    expect(await readArtifacts()).toEqual(before);
    expect(NodeFSP.link).not.toHaveBeenCalled();
    expect(NodeFS.watch).not.toHaveBeenCalled();
  }));

it("reads legacy equal-ID committed evidence without rewriting it on repeated restarts", async () =>
  fixture(async (root) => {
    const { input, files } = await committedTrial(root, true);
    const readArtifacts = () =>
      Promise.all([...files.keys()].map((path) => NodeFSP.readFile(path, "utf8")));
    const before = await readArtifacts();
    await awaitJonesTrialCommit(input);
    await awaitJonesTrialCommit(input);
    expect(await readArtifacts()).toEqual(before);
    expect(NodeFSP.link).not.toHaveBeenCalled();
    expect(NodeFS.watch).not.toHaveBeenCalled();
  }));

it.each([
  "new-claim",
  "different-artifact",
  "incomplete",
  "different-generation",
  "explicit-mismatch",
])("does not normalize missing staged handles for %s evidence", async (fault) =>
  fixture(async (root) => {
    const { input, files, journalPath, manifestPath } = await committedTrial(root, true);
    if (fault === "new-claim") {
      await NodeFSP.writeFile(
        NodePath.join(NodePath.dirname(journalPath), "prepare-intent.json"),
        JSON.stringify({ preparationClaimProtocol: 2 }),
      );
    } else {
      const path =
        fault === "different-generation"
          ? manifestPath
          : fault === "explicit-mismatch"
            ? input.commitGrantPath
            : journalPath;
      const value = JSON.parse(await NodeFSP.readFile(path, "utf8"));
      if (fault === "different-artifact") value.intent.staged.handle = "e".repeat(64);
      if (fault === "incomplete") value.phase = "resume-intent";
      if (fault === "different-generation") value.generation = "previous";
      if (fault === "explicit-mismatch") value.stagedHandle = "e".repeat(64);
      await NodeFSP.writeFile(path, JSON.stringify(value));
    }
    const readArtifacts = () =>
      Promise.all([...files.keys()].map((path) => NodeFSP.readFile(path, "utf8")));
    const before = await readArtifacts();
    await expect(awaitJonesTrialCommit(input)).rejects.toMatchObject({ step: "identity" });
    expect(await readArtifacts()).toEqual(before);
    expect(NodeFSP.link).not.toHaveBeenCalled();
    expect(NodeFS.watch).not.toHaveBeenCalled();
  }),
);

it.each([
  ["manifest", "owner", "other"],
  ["manifest", "generation", "another-generation"],
  ["manifest", "transactionId", "another-transaction"],
  ["manifest", "home", "another-home"],
  ["manifest", "databasePath", "another-database"],
  ["manifest", "profile", "another-profile"],
  ["manifest", "environmentId", "another-environment"],
  ["manifest", "sourceSha", "c".repeat(40)],
  ["manifest", "sourceTree", "d".repeat(40)],
  ["manifest", "version", "another-version"],
  ["grant", "generation", "another-generation"],
  ["grant", "sourceSha", "c".repeat(40)],
  ["grant", "stagedHandle", "c".repeat(64)],
  ["journal", "phase", "resume-intent"],
  ["journal", "phase", "blocked"],
  [
    "journal",
    "intent",
    { protocol: 1, transactionId: "synthetic-transaction", staged: { handle: "c".repeat(64) } },
  ],
  ["receipt", "resumeHeld", false],
  ["receipt", "stagedHandle", "c".repeat(64)],
  ["reservation", "transactionId", "another-transaction"],
  ["reservation", "backendProcess", { pid: 124, identity: "different backend" }],
])("holds committed restart when %s.%s differs", async (artifact, key, value) =>
  fixture(async (root) => {
    const { input, files, manifestPath, journalPath, reservationPath } = await committedTrial(root);
    const paths: Record<string, string> = {
      manifest: manifestPath,
      grant: input.commitGrantPath,
      journal: journalPath,
      receipt: input.trialReceiptPath,
      reservation: reservationPath,
    };
    const path = paths[artifact as string]!;
    const changed = JSON.parse(await NodeFSP.readFile(path, "utf8"));
    changed[key as string] = value;
    await NodeFSP.writeFile(path, JSON.stringify(changed));
    const readArtifacts = () =>
      Promise.all([...files.keys()].map((file) => NodeFSP.readFile(file, "utf8")));
    const before = await readArtifacts();
    await expect(awaitJonesTrialCommit(input)).rejects.toThrow();
    expect(await readArtifacts()).toEqual(before);
    expect(NodeFSP.link).not.toHaveBeenCalled();
  }),
);

it("holds a committed restart with missing completion evidence or a noncanonical descriptor", async () =>
  fixture(async (root) => {
    const { input, journalPath } = await committedTrial(root);
    const canonical = await NodeFSP.readFile(input.descriptorPath, "utf8");
    const anotherPath = NodePath.join(
      NodePath.dirname(input.descriptorPath),
      "other-descriptor.json",
    );
    await NodeFSP.writeFile(anotherPath, canonical);
    await expect(
      awaitJonesTrialCommit({ ...input, descriptorPath: anotherPath }),
    ).rejects.toMatchObject({ step: "identity" });
    await NodeFSP.unlink(journalPath);
    await expect(awaitJonesTrialCommit(input)).rejects.toMatchObject({ step: "identity" });
    expect(NodeFSP.link).not.toHaveBeenCalled();
  }));

it("removes only captured synthetic roots after success and callback failure", async () => {
  const completed = await fixture(async (root) => root);
  await expect(NodeFSP.lstat(completed)).rejects.toMatchObject({ code: "ENOENT" });
  let failed: string | undefined;
  const failure = new Error("Synthetic fixture failure");
  await expect(
    fixture(async (root) => {
      failed = root;
      throw failure;
    }),
  ).rejects.toBe(failure);
  expect(failed).toBeDefined();
  await expect(NodeFSP.lstat(failed!)).rejects.toMatchObject({ code: "ENOENT" });
});

it("cancels a waiting trial without reserving resume and removes its captured root", async () => {
  const captured = await fixture(async (root) => {
    const input = await trial(root);
    const controller = new AbortController();
    const gate = awaitJonesTrialCommit({ ...input, signal: controller.signal });
    try {
      await Promise.race([watchFile(input.trialReceiptPath, controller.signal), gate]);
      controller.abort();
      await expect(gate).rejects.toThrow("cancelled before commit");
      await NodeFSP.writeFile(
        input.commitGrantPath,
        JSON.stringify({ ...input, generation: input.transactionId }),
      );
      await expect(
        NodeFSP.lstat(NodePath.join(root, "resume-dispatched.json")),
      ).rejects.toMatchObject({
        code: "ENOENT",
      });
      return root;
    } finally {
      controller.abort();
      await Promise.allSettled([gate]);
    }
  });
  await expect(NodeFSP.lstat(captured)).rejects.toMatchObject({ code: "ENOENT" });
});

it("rejects a mismatched grant without reserving resume", async () =>
  fixture(async (root) => {
    const input = await trial(root);
    const controller = new AbortController();
    const gate = awaitJonesTrialCommit({ ...input, signal: controller.signal });
    try {
      await Promise.race([watchFile(input.trialReceiptPath, controller.signal), gate]);
      const rejected = expect(gate).rejects.toThrow("does not match");
      const pending = NodePath.join(root, "grant.pending");
      await NodeFSP.writeFile(
        pending,
        JSON.stringify({
          ...input,
          environmentId: "another-environment",
          generation: input.transactionId,
        }),
      );
      await NodeFSP.rename(pending, input.commitGrantPath);
      await rejected;
      await expect(
        NodeFSP.lstat(NodePath.join(root, "resume-dispatched.json")),
      ).rejects.toMatchObject({
        code: "ENOENT",
      });
    } finally {
      controller.abort();
      await Promise.allSettled([gate]);
    }
  }));

it("holds helper completion until the matching commit grant and reserves resume once", async () =>
  fixture(async (root) => {
    const descriptor = {
      protocol: 1,
      startupGateProtocol: 1,
      transactionId: "transaction",
      stagedHandle: "f".repeat(64),
      home: root,
      databasePath: NodePath.join(root, "userdata/statev2.sqlite"),
      profile: NodePath.join(root, "profile"),
      environmentId: "fixture",
      version: "0.0.44-preview.20261002.1",
      sourceSha: "a".repeat(40),
      sourceTree: "b".repeat(40),
      listener: "http://127.0.0.1:4888",
      trialReceiptPath: NodePath.join(root, "trial.json"),
      commitGrantPath: NodePath.join(root, "grant.json"),
    };
    const descriptorPath = NodePath.join(root, "descriptor.json");
    await NodeFSP.writeFile(descriptorPath, JSON.stringify(descriptor));
    let ready = false;
    const controller = new AbortController();
    const gate = awaitJonesTrialCommit({
      ...descriptor,
      observedListener: listener,
      descriptorPath,
      signal: controller.signal,
      buildMetadata: {
        jonesSource: {
          repository: "Jones-Systems/Jones-Code",
          sha: descriptor.sourceSha,
          tree: descriptor.sourceTree,
        },
      },
    }).then(() => {
      ready = true;
    });
    try {
      await Promise.race([watchFile(descriptor.trialReceiptPath, controller.signal), gate]);
      expect(ready).toBe(false);
      expect(
        JSON.parse(await NodeFSP.readFile(descriptor.trialReceiptPath, "utf8")).resumeHeld,
      ).toBe(true);
      await NodeFSP.writeFile(
        NodePath.join(root, "grant.pending"),
        JSON.stringify({ ...descriptor, generation: descriptor.transactionId }),
      );
      await NodeFSP.rename(NodePath.join(root, "grant.pending"), descriptor.commitGrantPath);
      await gate;
      expect(ready).toBe(true);
      expect(
        await NodeFSP.readFile(NodePath.join(root, "resume-dispatched.json"), "utf8"),
      ).toContain("transaction");
    } finally {
      controller.abort();
      await Promise.allSettled([gate]);
    }
  }));

it("rejects wrong source before receipt publication", async () =>
  fixture(async (root) => {
    const descriptorPath = NodePath.join(root, "descriptor.json");
    const descriptor = {
      protocol: 1,
      startupGateProtocol: 1,
      transactionId: "tx",
      stagedHandle: "f".repeat(64),
      home: root,
      databasePath: NodePath.join(root, "userdata/statev2.sqlite"),
      profile: NodePath.join(root, "profile"),
      environmentId: "fixture",
      version: "v",
      sourceSha: "a",
      sourceTree: "b",
      listener: "listener",
      trialReceiptPath: NodePath.join(root, "trial.json"),
      commitGrantPath: NodePath.join(root, "grant.json"),
    };
    await NodeFSP.writeFile(descriptorPath, JSON.stringify(descriptor));
    await expect(
      awaitJonesTrialCommit({
        ...descriptor,
        observedListener: listener,
        descriptorPath,
        buildMetadata: {
          jonesSource: { repository: "Jones-Systems/Jones-Code", sha: "wrong", tree: "b" },
        },
      }),
    ).rejects.toThrow("identity");
    await expect(NodeFSP.stat(descriptor.trialReceiptPath)).rejects.toMatchObject({
      code: "ENOENT",
    });
  }));

function latch() {
  let resolve!: () => void;
  const promise = new Promise<void>((complete) => {
    resolve = complete;
  });
  return { promise, resolve };
}

it.each(["valid", "mismatched"] as const)(
  "rechecks a %s grant after an initial miss when every filesystem event is dropped",
  async (mode) =>
    fixture(async (root) => {
      const input = await trial(root);
      const original = await vi.importActual<typeof NodeFSP>("node:fs/promises");
      const entered = latch();
      const release = latch();
      let inspections = 0;
      let closed = false;
      const watcher = Object.assign(new NodeEvents.EventEmitter(), {
        close: () => {
          closed = true;
        },
      });
      vi.mocked(NodeFS.watch).mockImplementation(() => watcher as NodeFS.FSWatcher);
      vi.mocked(NodeFSP.readFile).mockImplementation((async (
        ...args: Parameters<typeof NodeFSP.readFile>
      ) => {
        if (args[0] === input.commitGrantPath && ++inspections === 1) {
          entered.resolve();
          await release.promise;
          throw Object.assign(new Error("Synthetic initial grant miss"), { code: "ENOENT" });
        }
        return original.readFile(...args);
      }) as typeof NodeFSP.readFile);
      const controller = new AbortController();
      const gate = awaitJonesTrialCommit({ ...input, signal: controller.signal });
      const outcome = gate.then(
        () => ({ status: "committed" as const }),
        (cause: unknown) => ({ status: "rejected" as const, cause }),
      );
      try {
        await Promise.race([entered.promise, gate]);
        vi.useFakeTimers();
        await publishGrant(input.commitGrantPath, {
          ...input,
          generation: mode === "valid" ? input.transactionId : "different",
        });
        release.resolve();
        await vi.advanceTimersByTimeAsync(1_000);
        expect(inspections).toBeGreaterThan(1);
        const result = await outcome;
        if (mode === "valid") {
          expect(result.status).toBe("committed");
          expect(
            JSON.parse(
              await original.readFile(NodePath.join(root, "resume-dispatched.json"), "utf8"),
            ),
          ).toMatchObject({ resumeHeld: true, transactionId: input.transactionId });
        } else {
          expect(result).toMatchObject({
            status: "rejected",
            cause: { step: "grant", uncertain: false },
          });
          await noReservation(root);
        }
        expect(closed).toBe(true);
        expect(vi.getTimerCount()).toBe(0);
      } finally {
        release.resolve();
        controller.abort();
        await outcome;
        vi.useRealTimers();
      }
    }),
);

async function publishGrant(path: string, value: unknown) {
  const pending = `${path}.unpublished`;
  await NodeFSP.writeFile(pending, JSON.stringify(value));
  await NodeFSP.rename(pending, path);
}

async function noReservation(root: string) {
  await expect(NodeFSP.lstat(NodePath.join(root, "resume-dispatched.json"))).rejects.toMatchObject({
    code: "ENOENT",
  });
  expect((await NodeFSP.readdir(root)).filter((name) => name.endsWith(".pending"))).toEqual([]);
}

it.each([
  ["startup gate discriminator", { startupGateProtocol: undefined }],
  ["unsupported startup gate", { startupGateProtocol: 2 }],
  ["outer protocol", { protocol: 2 }],
  ["tree", { sourceTree: undefined }],
  ["version", { version: undefined }],
  ["listener", { listener: undefined }],
  ["different tree", { sourceTree: "c".repeat(40) }],
  ["different version", { version: "other" }],
] as const)("rejects descriptor %s before publishing a receipt", async (_label, change) =>
  fixture(async (root) => {
    const input = await trial(root);
    await NodeFSP.writeFile(input.descriptorPath, JSON.stringify({ ...input, ...change }));
    await expect(awaitJonesTrialCommit(input)).rejects.toMatchObject({
      step: "identity",
      uncertain: false,
    });
    await expect(NodeFSP.lstat(input.trialReceiptPath)).rejects.toMatchObject({ code: "ENOENT" });
    await noReservation(root);
  }),
);

it.each([
  ["startup gate discriminator", { startupGateProtocol: undefined }],
  ["unsupported startup gate", { startupGateProtocol: 2 }],
  ["outer protocol", { protocol: 2 }],
  ["transaction", { transactionId: "another" }],
  ["staged handle", { stagedHandle: "c".repeat(64) }],
  ["missing staged handle", { stagedHandle: undefined }],
  ["home", { home: "/different" }],
  ["database", { databasePath: "/different" }],
  ["profile", { profile: "/different" }],
  ["environment", { environmentId: "different" }],
  ["SHA", { sourceSha: "c".repeat(40) }],
  ["tree", { sourceTree: "c".repeat(40) }],
  ["missing tree", { sourceTree: undefined }],
  ["version", { version: "different" }],
  ["missing version", { version: undefined }],
  ["listener", { listener: "http://127.0.0.1:4889" }],
  ["missing listener", { listener: undefined }],
  ["generation", { generation: "different" }],
] as const)("rejects commit grant %s without reserving resume", async (_label, change) =>
  fixture(async (root) => {
    const input = await trial(root);
    const controller = new AbortController();
    const gate = awaitJonesTrialCommit({ ...input, signal: controller.signal });
    try {
      await Promise.race([watchFile(input.trialReceiptPath, controller.signal), gate]);
      const rejected = expect(gate).rejects.toMatchObject({ step: "grant", uncertain: false });
      await publishGrant(input.commitGrantPath, {
        ...input,
        generation: input.transactionId,
        ...change,
      });
      await rejected;
      await noReservation(root);
    } finally {
      controller.abort();
      await Promise.allSettled([gate]);
    }
  }),
);

it.each(["receipt", "reservation"])(
  "refuses an occupied %s without replacing it",
  async (artifact) =>
    fixture(async (root) => {
      const input = await trial(root);
      const occupied =
        artifact === "receipt"
          ? input.trialReceiptPath
          : NodePath.join(root, "resume-dispatched.json");
      await NodeFSP.writeFile(occupied, "prior exact bytes");
      await expect(awaitJonesTrialCommit(input)).rejects.toMatchObject({ step: "receipt" });
      expect(await NodeFSP.readFile(occupied, "utf8")).toBe("prior exact bytes");
      if (artifact === "reservation") {
        await expect(NodeFSP.lstat(input.trialReceiptPath)).rejects.toMatchObject({
          code: "ENOENT",
        });
      }
      expect((await NodeFSP.readdir(root)).filter((name) => name.endsWith(".pending"))).toEqual([]);
    }),
);

it.each([
  NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", 4889),
  NetAddress.inetAddressFromIpStringUnsafe("127.0.0.2", 4888),
  NetAddress.inetAddressFromIpStringUnsafe("::", 4888),
  NetAddress.unixPathAddress("/synthetic/socket"),
])("holds unsupported actual listener %s", async (observedListener) =>
  fixture(async (root) => {
    const input = await trial(root);
    await expect(awaitJonesTrialCommit({ ...input, observedListener })).rejects.toMatchObject({
      step: "listener",
    });
    await expect(NodeFSP.lstat(input.trialReceiptPath)).rejects.toMatchObject({ code: "ENOENT" });
  }),
);

it("cancels before any receipt I/O and distinguishes absent from empty descriptor", async () =>
  fixture(async (root) => {
    const input = await trial(root);
    const controller = new AbortController();
    controller.abort();
    await expect(
      awaitJonesTrialCommit({ ...input, signal: controller.signal }),
    ).rejects.toMatchObject({ step: "cancel" });
    await expect(
      awaitJonesTrialCommit({ ...input, descriptorPath: undefined }),
    ).resolves.toBeUndefined();
    await expect(awaitJonesTrialCommit({ ...input, descriptorPath: "" })).rejects.toMatchObject({
      step: "identity",
    });
    await expect(NodeFSP.lstat(input.trialReceiptPath)).rejects.toMatchObject({ code: "ENOENT" });
  }));

it("drains an in-flight grant inspection on cancellation and never reserves", async () =>
  fixture(async (root) => {
    const input = await trial(root);
    const original = await vi.importActual<typeof NodeFSP>("node:fs/promises");
    const entered = latch();
    const release = latch();
    vi.mocked(NodeFSP.readFile).mockImplementation((async (
      ...args: Parameters<typeof NodeFSP.readFile>
    ) => {
      if (args[0] === input.commitGrantPath) {
        entered.resolve();
        await release.promise;
      }
      return original.readFile(...args);
    }) as typeof NodeFSP.readFile);
    const controller = new AbortController();
    let settled = false;
    const gate = awaitJonesTrialCommit({ ...input, signal: controller.signal });
    void gate.then(
      () => {
        settled = true;
      },
      () => {
        settled = true;
      },
    );
    try {
      await Promise.race([entered.promise, gate]);
      controller.abort();
      expect(settled).toBe(false);
      await publishGrant(input.commitGrantPath, { ...input, generation: input.transactionId });
      release.resolve();
      await expect(gate).rejects.toMatchObject({ step: "cancel", uncertain: false });
      await noReservation(root);
    } finally {
      release.resolve();
      controller.abort();
      await Promise.allSettled([gate]);
    }
  }));

it("retains the resume marker and reports uncertainty when cancellation overlaps reservation", async () =>
  fixture(async (root) => {
    const input = await trial(root);
    const reservation = NodePath.join(root, "resume-dispatched.json");
    const original = await vi.importActual<typeof NodeFSP>("node:fs/promises");
    const entered = latch();
    const release = latch();
    vi.mocked(NodeFSP.link).mockImplementation(async (source, destination) => {
      await original.link(source, destination);
      if (destination === reservation) {
        entered.resolve();
        await release.promise;
      }
    });
    await publishGrant(input.commitGrantPath, { ...input, generation: input.transactionId });
    const controller = new AbortController();
    let settled = false;
    const gate = awaitJonesTrialCommit({ ...input, signal: controller.signal });
    void gate.then(
      () => {
        settled = true;
      },
      () => {
        settled = true;
      },
    );
    try {
      await Promise.race([entered.promise, gate]);
      controller.abort();
      expect(settled).toBe(false);
      release.resolve();
      await expect(gate).rejects.toMatchObject({ step: "reservation", uncertain: true });
      expect(JSON.parse(await NodeFSP.readFile(reservation, "utf8"))).toMatchObject({
        protocol: 1,
        startupGateProtocol: 1,
        sourceTree: input.sourceTree,
        version: input.version,
        listener: input.listener,
        transactionId: input.transactionId,
        resumeHeld: true,
      });
      await expect(awaitJonesTrialCommit(input)).rejects.toMatchObject({ step: "receipt" });
      expect((await NodeFSP.readdir(root)).filter((name) => name.endsWith(".pending"))).toEqual([]);
    } finally {
      release.resolve();
      controller.abort();
      await Promise.allSettled([gate]);
    }
  }));

it("retains a reservation when directory fsync fails and never treats it as activation", async () =>
  fixture(async (root) => {
    const input = await trial(root);
    const original = await vi.importActual<typeof NodeFSP>("node:fs/promises");
    let directoryOpens = 0;
    vi.mocked(NodeFSP.open).mockImplementation(async (path, flags, mode) => {
      const file = await original.open(path, flags, mode);
      if (path === root && ++directoryOpens === 2) {
        file.sync = async () => {
          throw new Error("Synthetic reservation fsync failure");
        };
      }
      return file;
    });
    await publishGrant(input.commitGrantPath, { ...input, generation: input.transactionId });
    await expect(awaitJonesTrialCommit(input)).rejects.toMatchObject({
      step: "reservation",
      uncertain: true,
    });
    expect(
      JSON.parse(await NodeFSP.readFile(NodePath.join(root, "resume-dispatched.json"), "utf8"))
        .resumeHeld,
    ).toBe(true);
    expect((await NodeFSP.readdir(root)).filter((name) => name.endsWith(".pending"))).toEqual([]);
  }));

it("publishes full identity and exact backend proof before accepting one grant", async () =>
  fixture(async (root) => {
    const input = await trial(root);
    const unrelated = NodePath.join(root, ".another-owner.pending");
    await NodeFSP.writeFile(unrelated, "another invocation");
    const controller = new AbortController();
    const gate = awaitJonesTrialCommit({
      ...input,
      observedListener: NetAddress.inetAddressFromIpStringUnsafe("0.0.0.0", 4888),
      signal: controller.signal,
    });
    try {
      await Promise.race([watchFile(input.trialReceiptPath, controller.signal), gate]);
      const receipt = JSON.parse(await NodeFSP.readFile(input.trialReceiptPath, "utf8"));
      const {
        descriptorPath: _path,
        buildMetadata: _build,
        trialReceiptPath: _receipt,
        commitGrantPath: _grant,
        observedListener: _socket,
        ...identity
      } = input;
      expect(receipt).toMatchObject({ ...identity, resumeHeld: true });
      expect(receipt.backendProcess.pid).toBe(process.pid);
      expect(receipt.backendProcess.identity).toMatch(/\S/);
      await expect(
        NodeFSP.lstat(NodePath.join(root, "resume-dispatched.json")),
      ).rejects.toMatchObject({ code: "ENOENT" });
      await publishGrant(input.commitGrantPath, { ...identity, generation: input.transactionId });
      await gate;
      expect(
        JSON.parse(await NodeFSP.readFile(NodePath.join(root, "resume-dispatched.json"), "utf8")),
      ).toEqual(receipt);
      expect(await NodeFSP.readFile(unrelated, "utf8")).toBe("another invocation");
      expect((await NodeFSP.readdir(root)).filter((name) => name.endsWith(".pending"))).toEqual([
        NodePath.basename(unrelated),
      ]);
      await expect(awaitJonesTrialCommit(input)).rejects.toMatchObject({ step: "receipt" });
    } finally {
      controller.abort();
      await Promise.allSettled([gate]);
    }
  }));
