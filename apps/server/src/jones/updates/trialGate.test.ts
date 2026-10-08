// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFSP from "node:fs/promises";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import { afterEach, expect, it, vi } from "vite-plus/test";
import * as NetAddress from "effect/net/NetAddress";
import { awaitJonesTrialCommit } from "./trialGate.ts";
import { prepareNativeContinuationReceipt } from "./nativePreparation.ts";

vi.mock("node:fs/promises", async (importOriginal) => {
  const original = await importOriginal<typeof NodeFSP>();
  return {
    ...original,
    readFile: vi.fn(original.readFile),
    link: vi.fn(original.link),
    open: vi.fn(original.open),
  };
});
afterEach(async () => {
  const original = await vi.importActual<typeof NodeFSP>("node:fs/promises");
  vi.mocked(NodeFSP.readFile).mockReset().mockImplementation(original.readFile);
  vi.mocked(NodeFSP.link).mockReset().mockImplementation(original.link);
  vi.mocked(NodeFSP.open).mockReset().mockImplementation(original.open);
});

const listener = NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", 4888);

async function fixture<A>(body: (root: string) => Promise<A>): Promise<A> {
  const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-trial-test-"));
  try {
    await NodeFSP.mkdir(NodePath.join(root, "userdata"));
    await NodeFSP.mkdir(NodePath.join(root, "profile"));
    await NodeFSP.writeFile(NodePath.join(root, "userdata/statev2.sqlite"), "state");
    return await body(root);
  } finally {
    await NodeFSP.rm(root, { recursive: true, force: true });
    await expect(NodeFSP.lstat(root)).rejects.toMatchObject({ code: "ENOENT" });
  }
}
async function watchFile(file: string, signal: AbortSignal) {
  const watcher = NodeFS.watch(NodePath.dirname(file));
  let rejectWait: ((cause: Error) => void) | undefined;
  const aborted = () => rejectWait?.(new Error("Synthetic receipt observation cancelled."));
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
      signal.addEventListener("abort", aborted, { once: true });
      if (signal.aborted) aborted();
      else inspect();
    });
  } finally {
    signal.removeEventListener("abort", aborted);
    watcher.close();
  }
}

async function trial(root: string) {
  const descriptor = {
    protocol: 1,
    startupGateProtocol: 1,
    transactionId: "synthetic-transaction",
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

it("prepares only once for one fixed native transaction", async () =>
  fixture(async (root) => {
    await NodeFSP.mkdir(NodePath.join(root, "runtime"));
    await NodeFSP.writeFile(
      NodePath.join(root, "runtime/jones-active-install.json"),
      JSON.stringify({
        protocol: 1,
        owner: "desktop",
        home: root,
        databasePath: NodePath.join(root, "userdata/statev2.sqlite"),
        profile: NodePath.join(root, "profile"),
        environmentId: "fixture",
        version: "v",
      }),
    );
    let prepares = 0;
    const input = {
      home: root,
      databasePath: NodePath.join(root, "userdata/statev2.sqlite"),
      environmentId: "fixture",
      version: "v",
      handle: "c".repeat(64),
      prepare: async () => {
        prepares++;
        return [];
      },
      clear: async () => {},
    };
    await prepareNativeContinuationReceipt(input);
    await prepareNativeContinuationReceipt(input);
    expect(prepares).toBe(1);
    await expect(
      prepareNativeContinuationReceipt({ ...input, environmentId: "wrong" }),
    ).rejects.toThrow("manifest");
    expect(prepares).toBe(1);
  }));

it("retains an uncertain preparation reservation and refuses replay", async () =>
  fixture(async (root) => {
    await NodeFSP.mkdir(NodePath.join(root, "runtime"));
    await NodeFSP.writeFile(
      NodePath.join(root, "runtime/jones-active-install.json"),
      JSON.stringify({
        protocol: 1,
        owner: "desktop",
        home: root,
        databasePath: NodePath.join(root, "userdata/statev2.sqlite"),
        profile: NodePath.join(root, "profile"),
        environmentId: "fixture",
        version: "v",
      }),
    );
    let prepares = 0;
    const input = {
      home: root,
      databasePath: NodePath.join(root, "userdata/statev2.sqlite"),
      environmentId: "fixture",
      version: "v",
      handle: "d".repeat(64),
      prepare: async () => {
        prepares++;
        throw new Error("Unknown effect");
      },
      clear: async () => {},
    };
    await expect(prepareNativeContinuationReceipt(input)).rejects.toThrow("Unknown effect");
    await expect(prepareNativeContinuationReceipt(input)).rejects.toMatchObject({ code: "EEXIST" });
    expect(prepares).toBe(1);
  }));

function latch() {
  let resolve!: () => void;
  const promise = new Promise<void>((complete) => {
    resolve = complete;
  });
  return { promise, resolve };
}

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
