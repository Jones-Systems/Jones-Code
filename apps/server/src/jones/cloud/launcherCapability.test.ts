// @effect-diagnostics nodeBuiltinImport:off
// Synthetic receipts exercise ownership and exact process bindings without starting a host service.
import { assert, it } from "@effect/vitest";
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import {
  publishLauncherCapabilityReceipt,
  readLauncherCapabilityReceipt,
  retractLauncherCapabilityReceipt,
  type LauncherCapabilityReceipt,
} from "./launcherCapability.ts";

async function fixture(body: (base: string, receipt: LauncherCapabilityReceipt) => Promise<void>) {
  const allocated = await NodeFSP.mkdtemp(
    NodePath.join(NodeOS.tmpdir(), "jones-launcher-capability-test-"),
  );
  try {
    const base = await NodeFSP.realpath(allocated);
    await NodeFSP.mkdir(NodePath.join(base, "runtime"), { mode: 0o700 });
    await body(base, {
      schema: 1,
      baseDir: base,
      launcherVersion: "0.0.0-preview.20261002.101.1",
      launcherPid: 101,
      launcherProtocol: 4,
      qualifiedUpdatesProtocol: 1,
      startupGateProtocol: 1,
      childPid: 102,
      childVersion: "0.0.0-preview.20261002.100",
    });
  } finally {
    await NodeFSP.rm(allocated, { recursive: true, force: true });
  }
}

const owner = NodeOS.userInfo().uid;

it("publishes owner-only evidence and checks both exact live process identities", async () => {
  await fixture(async (base, receipt) => {
    const calls: number[] = [];
    const guard = {
      uid: owner,
      isOwnedLive: async (pid: number) => {
        calls.push(pid);
        return true;
      },
    };
    assert.equal(await readLauncherCapabilityReceipt(base, receipt, guard), undefined);
    await publishLauncherCapabilityReceipt(receipt);
    const file = NodePath.join(base, "runtime", "jones-launcher-capability.json");
    assert.equal((await NodeFSP.stat(file)).mode & 0o777, 0o600);
    assert.deepEqual(await readLauncherCapabilityReceipt(base, receipt, guard), receipt);
    assert.deepEqual(calls, [101, 102]);
    assert.deepEqual(await NodeFSP.readdir(NodePath.dirname(file)), [NodePath.basename(file)]);
    await retractLauncherCapabilityReceipt(base, 101, 102);
    assert.equal(await readLauncherCapabilityReceipt(base, receipt, guard), undefined);
    await retractLauncherCapabilityReceipt(base, 101, 102);
  });
});

it("refuses stale launcher or child identity and nonlive native ownership evidence", async () => {
  await fixture(async (base, receipt) => {
    await publishLauncherCapabilityReceipt(receipt);
    let probes = 0;
    const guard = {
      uid: owner,
      isOwnedLive: async () => {
        probes += 1;
        return true;
      },
    };
    for (const change of [
      { launcherPid: 103 },
      { childPid: 104 },
      { launcherVersion: "different" },
      { childVersion: "different" },
    ])
      assert.equal(
        await readLauncherCapabilityReceipt(base, { ...receipt, ...change }, guard),
        undefined,
      );
    assert.equal(probes, 0);
    for (const dead of [101, 102]) {
      assert.equal(
        await readLauncherCapabilityReceipt(base, receipt, {
          uid: owner,
          isOwnedLive: async (pid) => pid !== dead,
        }),
        undefined,
      );
    }
    await NodeAssert.rejects(retractLauncherCapabilityReceipt(base, 101, 999), /another child/);
    assert.deepEqual(await readLauncherCapabilityReceipt(base, receipt, guard), receipt);
  });
});

it("rejects malformed, foreign-home, foreign-owner, permissive and symlink receipts", async () => {
  await fixture(async (base, receipt) => {
    const file = NodePath.join(base, "runtime", "jones-launcher-capability.json");
    const guard = { uid: owner, isOwnedLive: async () => true };
    for (const change of [
      { schema: 2 },
      { startupGateProtocol: 2 },
      { childPid: 101 },
      { baseDir: "/different" },
    ]) {
      await NodeFSP.writeFile(file, JSON.stringify({ ...receipt, ...change }), { mode: 0o600 });
      await NodeAssert.rejects(readLauncherCapabilityReceipt(base, receipt, guard));
      await NodeAssert.rejects(publishLauncherCapabilityReceipt(receipt));
    }
    await NodeFSP.writeFile(file, JSON.stringify(receipt), { mode: 0o600 });
    await NodeAssert.rejects(
      readLauncherCapabilityReceipt(base, receipt, { ...guard, uid: owner + 1 }),
      /ownership/,
    );
    await NodeFSP.chmod(file, 0o644);
    await NodeAssert.rejects(readLauncherCapabilityReceipt(base, receipt, guard), /ownership/);
    await NodeAssert.rejects(publishLauncherCapabilityReceipt(receipt), /ownership/);
    await NodeFSP.unlink(file);
    const foreign = NodePath.join(base, "foreign");
    await NodeFSP.writeFile(foreign, JSON.stringify(receipt), { mode: 0o600 });
    await NodeFSP.symlink(foreign, file);
    await NodeAssert.rejects(readLauncherCapabilityReceipt(base, receipt, guard));
    await NodeAssert.rejects(publishLauncherCapabilityReceipt(receipt));
    assert.equal(await NodeFSP.readFile(foreign, "utf8"), JSON.stringify(receipt));
  });
});

it("replaces an owned prior child receipt without trusting it as live evidence", async () => {
  await fixture(async (base, receipt) => {
    await publishLauncherCapabilityReceipt(receipt);
    const next = { ...receipt, childPid: 103, childVersion: "0.0.0-preview.20261002.101.1" };
    await publishLauncherCapabilityReceipt(next);
    const guard = { uid: owner, isOwnedLive: async () => true };
    assert.equal(await readLauncherCapabilityReceipt(base, receipt, guard), undefined);
    assert.deepEqual(await readLauncherCapabilityReceipt(base, next, guard), next);
    await NodeAssert.rejects(retractLauncherCapabilityReceipt(base, 101, 102), /another child/);
    await retractLauncherCapabilityReceipt(base, 101, 103);
  });
});
