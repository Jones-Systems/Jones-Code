// @effect-diagnostics nodeBuiltinImport:off - Synthetic source integration, no packaged/native claim.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";
import { afterEach, expect, it, vi } from "vite-plus/test";
import { JonesActionsClient } from "@t3tools/shared/jones/jonesActions";
import { createPickerVerificationCore } from "./pickerVerificationCore.ts";
import { createDesktopUpdateBridge } from "./updateBridge.ts";
import { createMemoryIpc } from "../../../../../scripts/jones/updates/packaged-picker-memory-ipc.mjs";
import {
  createPickerFixture,
  createPickerObservations,
  runPickerScenario,
} from "../../../../../scripts/jones/updates/packaged-picker-scenario.mjs";

vi.mock("./jonesMacStagingChecks.ts", async (original) => ({
  ...(await original<typeof import("./jonesMacStagingChecks.ts")>()),
  readMacBundleIdentity: async (appPath: string) => ({
    bundleIdentifier: "com.t3tools.t3code",
    version: "0.0.44-preview.20261116.1.1",
    executablePath: NodePath.join(appPath, "Contents/MacOS/Fixture"),
  }),
}));
const roots: string[] = [];
afterEach(async () => {
  for (const root of roots.splice(0)) {
    await NodeFSP.rm(root, { recursive: true });
    await expect(NodeFSP.lstat(root)).rejects.toMatchObject({ code: "ENOENT" });
  }
});
it.each(["unchanged", "wrong-latest", "same-id-different-identity"] as const)(
  "runs the real composition scenario and discriminates dispatched identity: %s",
  async (mutation) => {
    const latest = await new JonesActionsClient({
      transport: createPickerFixture("1".repeat(40)).transport,
    }).check({ installedSource: "1".repeat(40), platform: "darwin", architecture: "arm64" });
    if (latest.state !== "available") throw new Error("Latest fixture failed real qualification.");
    const parent = NodePath.resolve("node_modules/.cache/jones/packaged-picker-tests");
    await NodeFSP.mkdir(parent, { recursive: true });
    const root = await NodeFSP.mkdtemp(NodePath.join(parent, "scenario-"));
    roots.push(root);
    const nonce = NodeCrypto.randomBytes(16).toString("hex"),
      home = NodePath.join(root, "home"),
      profile = NodePath.join(home, "profile"),
      appPath = NodePath.join(root, "Applications/Fixture.app"),
      appRoot = NodePath.join(appPath, "Contents/Resources/app");
    await NodeFSP.mkdir(profile, { recursive: true });
    await NodeFSP.mkdir(NodePath.join(home, "userdata"));
    await NodeFSP.mkdir(appRoot, { recursive: true });
    await NodeFSP.mkdir(NodePath.join(appPath, "Contents/MacOS"));
    await NodeFSP.writeFile(NodePath.join(root, ".picker-owner"), nonce);
    await NodeFSP.writeFile(NodePath.join(home, "userdata/statev2.sqlite"), "");
    await NodeFSP.writeFile(NodePath.join(home, "userdata/environment-id"), "picker-synthetic");
    await NodeFSP.writeFile(NodePath.join(appPath, "Contents/MacOS/Fixture"), "");
    await NodeFSP.writeFile(
      NodePath.join(appRoot, "package.json"),
      JSON.stringify({
        version: "0.0.44-preview.20261116.1.1",
        jonesSource: {
          repository: "Jones-Systems/Jones-Code",
          sha: "1".repeat(40),
          tree: "2".repeat(40),
        },
      }),
    );
    const results = [];
    for (const phase of mutation === "unchanged"
      ? (["catalog", "persisted", "inflight", "held"] as const)
      : (["catalog"] as const)) {
      const observations = createPickerObservations(),
        fixture = createPickerFixture("1".repeat(40), () => {
          if (mutation === "wrong-latest")
            fixture.jobs[0]!.candidate = structuredClone(latest.candidate);
          if (mutation === "same-id-different-identity")
            fixture.jobs[0]!.candidate = {
              ...fixture.jobs[0]!.candidate,
              artifactDigest: "8".repeat(64),
            };
          observations.notify();
        }),
        ipc = createMemoryIpc(),
        events: unknown[] = [];
      const core = await createPickerVerificationCore({
        root,
        nonce,
        home,
        profile,
        appPath,
        appRoot,
        executablePath: NodePath.join(appPath, "Contents/MacOS/Fixture"),
        version: "0.0.44-preview.20261116.1.1",
        phase,
        transport: fixture.transport,
        stagingWorker: fixture.worker,
        broadcastState: (state) => ipc.send("desktop:update-state", state),
        broadcastPlan: async (state) => {
          await ipc.send("desktop:jones-install-plan-state", state);
          observations.notify();
        },
      });
      try {
        await core.register(ipc.main);
        const bridge = createDesktopUpdateBridge(ipc.renderer);
        bridge.onUpdateState((value) => events.push({ kind: "state", value }));
        bridge.onInstallPlan!((value) => events.push({ kind: "plan", value }));
        const host = {
          ...core,
          installCalls: ipc.installCalls,
          bridgeCall: (member: keyof typeof bridge, args: unknown[] = []) =>
            Reflect.apply(bridge[member]!, undefined, args),
          inspect: async () => ({
            members: Object.keys(bridge).sort(),
            require: "undefined",
            process: "undefined",
            events: events.splice(0),
          }),
          waitForJobs: (count: number) =>
            observations.wait(
              () => fixture.jobs.length >= count,
              async () => fixture.jobs,
            ),
          waitForPlan: (predicate: (value: unknown) => boolean) =>
            observations.wait(predicate, () => bridge.installPlan!({ type: "read" })),
          assertNoTransactions: async () => {
            await expect(
              NodeFSP.readdir(NodePath.join(home, "runtime/jones-updates/transactions")),
            ).rejects.toMatchObject({ code: "ENOENT" });
          },
        };
        if (mutation === "unchanged") results.push(await runPickerScenario(phase, host, fixture));
        else
          await expect(runPickerScenario(phase, host, fixture)).rejects.toThrow(
            "PREP-A1-exact-artifact",
          );
      } finally {
        await core.close();
      }
    }
    if (mutation === "unchanged")
      expect(results.map((result) => result.phase)).toEqual([
        "catalog",
        "persisted",
        "inflight",
        "held",
      ]);
  },
);
