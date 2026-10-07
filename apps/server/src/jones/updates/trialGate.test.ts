// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFSP from "node:fs/promises";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import { expect, it } from "vite-plus/test";
import { awaitJonesTrialCommit } from "./trialGate.ts";
import { prepareNativeContinuationReceipt } from "./nativePreparation.ts";

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
