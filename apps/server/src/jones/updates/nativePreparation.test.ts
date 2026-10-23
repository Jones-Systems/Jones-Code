// @effect-diagnostics nodeBuiltinImport:off -- Synthetic protocol files are removed by their creating fixture.
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { expect, it } from "vite-plus/test";
import {
  NativePreparationRefusal,
  prepareNativeContinuationReceipt,
  recordNativePreparationRefusal,
} from "./nativePreparation.ts";

async function fixture(body: (value: Awaited<ReturnType<typeof setup>>) => Promise<void>) {
  const temporary = await NodeFSP.mkdtemp(
    NodePath.join(NodeOS.tmpdir(), "jones-native-preparation-"),
  );
  try {
    await body(await setup(await NodeFSP.realpath(temporary)));
  } finally {
    await NodeFSP.rm(temporary, { recursive: true, force: true });
    await expect(NodeFSP.lstat(temporary)).rejects.toMatchObject({ code: "ENOENT" });
  }
}

async function setup(home: string) {
  const handle = "c".repeat(64);
  const transactionId = "d".repeat(64);
  const databasePath = NodePath.join(home, "userdata", "statev2.sqlite");
  const profile = NodePath.join(home, "profile");
  const directory = NodePath.join(home, "runtime", "jones-updates", "transactions", transactionId);
  const selectionPath = NodePath.join(
    home,
    "runtime",
    "jones-updates",
    "staging",
    `${"a".repeat(40)}-fixture.json`,
  );
  await NodeFSP.mkdir(NodePath.dirname(databasePath), { recursive: true });
  await NodeFSP.mkdir(profile);
  await NodeFSP.mkdir(directory, { recursive: true });
  await NodeFSP.mkdir(NodePath.dirname(selectionPath));
  await NodeFSP.writeFile(databasePath, "opaque state");
  const active = {
    protocol: 1,
    owner: "desktop",
    home,
    databasePath,
    profile,
    environmentId: "fixture",
    version: "v",
    sourceSha: "a".repeat(40),
  };
  await NodeFSP.writeFile(
    NodePath.join(home, "runtime", "jones-active-install.json"),
    JSON.stringify(active),
  );
  const selection = {
    schema: 1,
    source: "jones-actions",
    home,
    profile,
    currentVersion: "v",
    installedSource: active.sourceSha,
    active,
    app: { handle },
  };
  const raw = JSON.stringify(selection) + "\n";
  await NodeFSP.writeFile(selectionPath, raw);
  const claim = {
    protocol: 1,
    preparationClaimProtocol: 2,
    stagedHandle: handle,
    transactionId,
    selectionPath,
    selectionSha256: NodeCrypto.createHash("sha256").update(raw).digest("hex"),
    home,
    databasePath,
    profile,
    environmentId: "fixture",
  };
  const claimPath = NodePath.join(directory, "prepare-intent.json");
  await NodeFSP.writeFile(claimPath, JSON.stringify(claim));
  let prepares = 0;
  const input = {
    home,
    databasePath,
    environmentId: "fixture",
    version: "v",
    handle,
    transactionId,
    prepare: async () => {
      expect(
        JSON.parse(
          await NodeFSP.readFile(NodePath.join(directory, "prepare-dispatched.json"), "utf8"),
        ),
      ).toEqual(claim);
      prepares++;
      return ["thread"];
    },
    clear: async () => {},
  };
  return { home, directory, selectionPath, claimPath, claim, input, prepares: () => prepares };
}

it.each([2, 3])(
  "prepares claim protocol %s once after the durable dispatch record",
  async (protocol) =>
    fixture(async ({ directory, claim, input, prepares }) => {
      claim.preparationClaimProtocol = protocol;
      await NodeFSP.writeFile(
        NodePath.join(directory, "prepare-intent.json"),
        JSON.stringify(claim),
      );
      await prepareNativeContinuationReceipt(input);
      await prepareNativeContinuationReceipt(input);
      expect(prepares()).toBe(1);
      expect(
        JSON.parse(await NodeFSP.readFile(NodePath.join(directory, "continuation.json"), "utf8")),
      ).toEqual({ ...claim, prepared: true });
      await expect(
        prepareNativeContinuationReceipt({ ...input, environmentId: "wrong" }),
      ).rejects.toThrow("manifest");
      expect(prepares()).toBe(1);
    }),
);

it.each(["before-dispatch", "after-receipt"])(
  "refuses a withdrawn preparation %s without another side effect",
  async (phase) =>
    fixture(async ({ directory, input, prepares }) => {
      if (phase === "after-receipt") await prepareNativeContinuationReceipt(input);
      await NodeFSP.writeFile(NodePath.join(directory, "preparation-withdrawal.json"), "held");
      await expect(prepareNativeContinuationReceipt(input)).rejects.toThrow("withdrawn");
      expect(prepares()).toBe(phase === "after-receipt" ? 1 : 0);
    }),
);

it("clears returned continuation markers when withdrawal races preparation", async () =>
  fixture(async ({ directory, input, prepares }) => {
    const cleared: Array<ReadonlyArray<string>> = [];
    await expect(
      prepareNativeContinuationReceipt({
        ...input,
        prepare: async () => {
          const marked = await input.prepare();
          await NodeFSP.writeFile(NodePath.join(directory, "preparation-withdrawal.json"), "held");
          return marked;
        },
        clear: async (threadIds) => {
          cleared.push(threadIds);
        },
      }),
    ).rejects.toThrow("withdrawn");
    expect(prepares()).toBe(1);
    expect(cleared).toEqual([["thread"]]);
    await expect(NodeFSP.stat(NodePath.join(directory, "continuation.json"))).rejects.toMatchObject(
      { code: "ENOENT" },
    );
    await expect(
      NodeFSP.stat(NodePath.join(directory, "prepare-refused.json")),
    ).rejects.toMatchObject({ code: "ENOENT" });
  }));

it("retains uncertain preparation dispatch and refuses replay", async () =>
  fixture(async ({ directory, input }) => {
    let prepares = 0;
    const uncertain = {
      ...input,
      prepare: async () => {
        prepares++;
        throw new Error("Unknown effect");
      },
    };
    await expect(prepareNativeContinuationReceipt(uncertain)).rejects.toThrow("Unknown effect");
    await expect(prepareNativeContinuationReceipt(uncertain)).rejects.toMatchObject({
      code: "EEXIST",
    });
    expect(prepares).toBe(1);
    await expect(NodeFSP.stat(NodePath.join(directory, "continuation.json"))).rejects.toMatchObject(
      { code: "ENOENT" },
    );
  }));

it.each([
  "missing-claim",
  "legacy-claim",
  "wrong-protocol",
  "wrong-transaction",
  "wrong-selection-hash",
  "wrong-selection-handle",
  "discarded-selection",
  "wrong-selection-path",
])("refuses %s before any continuation side effect", async (fault) =>
  fixture(async ({ directory, selectionPath, claimPath, claim, input, prepares }) => {
    if (fault === "missing-claim") await NodeFSP.unlink(claimPath);
    if (fault === "legacy-claim") {
      await NodeFSP.writeFile(
        claimPath,
        JSON.stringify({
          protocol: 1,
          transactionId: claim.transactionId,
          home: claim.home,
          databasePath: claim.databasePath,
          profile: claim.profile,
          environmentId: claim.environmentId,
          prepared: true,
        }),
      );
    }
    if (fault === "wrong-protocol")
      await NodeFSP.writeFile(claimPath, JSON.stringify({ ...claim, preparationClaimProtocol: 1 }));
    if (fault === "wrong-transaction")
      await NodeFSP.writeFile(claimPath, JSON.stringify({ ...claim, transactionId: input.handle }));
    if (fault === "wrong-selection-hash")
      await NodeFSP.writeFile(selectionPath, (await NodeFSP.readFile(selectionPath, "utf8")) + " ");
    if (fault === "wrong-selection-handle") {
      const value = JSON.parse(await NodeFSP.readFile(selectionPath, "utf8"));
      value.app.handle = "d".repeat(64);
      const raw = JSON.stringify(value);
      await NodeFSP.writeFile(selectionPath, raw);
      await NodeFSP.writeFile(
        claimPath,
        JSON.stringify({
          ...claim,
          selectionSha256: NodeCrypto.createHash("sha256").update(raw).digest("hex"),
        }),
      );
    }
    if (fault === "discarded-selection") await NodeFSP.unlink(selectionPath);
    if (fault === "wrong-selection-path")
      await NodeFSP.writeFile(
        claimPath,
        JSON.stringify({ ...claim, selectionPath: NodePath.join(directory, "outside.json") }),
      );
    await expect(prepareNativeContinuationReceipt(input)).rejects.toBeInstanceOf(
      NativePreparationRefusal,
    );
    expect(prepares()).toBe(0);
    await expect(
      NodeFSP.stat(NodePath.join(directory, "prepare-dispatched.json")),
    ).rejects.toMatchObject({ code: "ENOENT" });
    // Only a readable claim bound to this request can carry the refusal proof.
    const refusal = NodePath.join(directory, "prepare-refused.json");
    if (["missing-claim", "legacy-claim", "wrong-transaction"].includes(fault)) {
      await expect(NodeFSP.stat(refusal)).rejects.toMatchObject({ code: "ENOENT" });
      return;
    }
    const claimRaw = await NodeFSP.readFile(claimPath);
    expect(JSON.parse(await NodeFSP.readFile(refusal, "utf8"))).toEqual({
      protocol: 1,
      transactionId: claim.transactionId,
      stagedHandle: input.handle,
      claimSha256: NodeCrypto.createHash("sha256").update(claimRaw).digest("hex"),
      reason: expect.stringMatching(
        fault === "discarded-selection" ? /could not be validated \(ENOENT\)/ : /^Native|^The/,
      ),
    });
    // A recorded refusal fences any later dispatch for the same claim.
    await NodeFSP.writeFile(claimPath, JSON.stringify(claim));
    await expect(prepareNativeContinuationReceipt(input)).rejects.toBeInstanceOf(
      NativePreparationRefusal,
    );
    expect(prepares()).toBe(0);
  }),
);

it("records no refusal once dispatch may have happened", async () =>
  fixture(async ({ directory, input }) => {
    const failing = {
      ...input,
      prepare: async () => {
        throw new Error("Unknown effect");
      },
    };
    await expect(prepareNativeContinuationReceipt(failing)).rejects.toThrow("Unknown effect");
    await expect(prepareNativeContinuationReceipt(failing)).rejects.toMatchObject({
      code: "EEXIST",
    });
    await expect(
      NodeFSP.stat(NodePath.join(directory, "prepare-refused.json")),
    ).rejects.toMatchObject({ code: "ENOENT" });
  }));

it("leaves a retired claim untouched when its late request arrives", async () =>
  fixture(async ({ home, directory, input, prepares }) => {
    const retired = NodePath.join(home, "runtime", "jones-updates", "retired-preparations");
    await NodeFSP.mkdir(retired);
    await NodeFSP.rename(directory, NodePath.join(retired, input.transactionId));
    await expect(prepareNativeContinuationReceipt(input)).rejects.toBeInstanceOf(
      NativePreparationRefusal,
    );
    await recordNativePreparationRefusal({ ...input, reason: "late" });
    expect(prepares()).toBe(0);
    await expect(NodeFSP.lstat(directory)).rejects.toMatchObject({ code: "ENOENT" });
    expect(await NodeFSP.readdir(NodePath.join(retired, input.transactionId))).toEqual([
      "prepare-intent.json",
    ]);
  }));

it("admits only one of two concurrent backend preparation calls", async () =>
  fixture(async ({ input }) => {
    let calls = 0;
    let enter!: () => void;
    let release!: () => void;
    const entered = new Promise<void>((resolve) => {
      enter = resolve;
    });
    const released = new Promise<void>((resolve) => {
      release = resolve;
    });
    const concurrent = {
      ...input,
      prepare: async () => {
        calls++;
        enter();
        await released;
        return [];
      },
    };
    const first = prepareNativeContinuationReceipt(concurrent);
    try {
      await Promise.race([
        entered,
        first.then(() => {
          throw new Error("Preparation bypassed its barrier.");
        }),
      ]);
      await expect(prepareNativeContinuationReceipt(concurrent)).rejects.toMatchObject({
        code: "EEXIST",
      });
    } finally {
      release();
      await first;
    }
    expect(calls).toBe(1);
  }));
