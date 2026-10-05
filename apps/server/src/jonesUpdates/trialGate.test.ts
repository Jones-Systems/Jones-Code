// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFSP from "node:fs/promises";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import { expect, it } from "vite-plus/test";
import { awaitJonesTrialCommit } from "./trialGate.ts";
import { prepareNativeContinuationReceipt } from "./nativePreparation.ts";

async function fixture(body: (root: string) => Promise<void>) {
  const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-trial-test-"));
  try {
    await NodeFSP.mkdir(NodePath.join(root, "userdata"));
    await NodeFSP.mkdir(NodePath.join(root, "profile"));
    await NodeFSP.writeFile(NodePath.join(root, "userdata/statev2.sqlite"), "state");
    await body(root);
  } finally {
    await NodeFSP.rm(root, { recursive: true, force: true });
  }
}
async function watchFile(file: string) {
  const watcher = NodeFS.watch(NodePath.dirname(file));
  try {
    await new Promise<void>((resolve, reject) => {
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
      inspect();
    });
  } finally {
    watcher.close();
  }
}
it("holds native readiness until the matching commit grant and reserves resume once", async () =>
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
    const gate = awaitJonesTrialCommit({
      ...descriptor,
      descriptorPath,
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
    await watchFile(descriptor.trialReceiptPath);
    expect(ready).toBe(false);
    expect(JSON.parse(await NodeFSP.readFile(descriptor.trialReceiptPath, "utf8")).resumeHeld).toBe(
      true,
    );
    await NodeFSP.writeFile(
      NodePath.join(root, "grant.pending"),
      JSON.stringify({ ...descriptor, generation: descriptor.transactionId }),
    );
    await NodeFSP.rename(NodePath.join(root, "grant.pending"), descriptor.commitGrantPath);
    await gate;
    expect(ready).toBe(true);
    expect(await NodeFSP.readFile(NodePath.join(root, "resume-dispatched.json"), "utf8")).toContain(
      "transaction",
    );
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
