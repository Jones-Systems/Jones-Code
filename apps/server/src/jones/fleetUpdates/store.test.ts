// @effect-diagnostics nodeBuiltinImport:off -- Synthetic native journal fixtures exercise filesystem custody and retain exact-root cleanup.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import { describe, expect, it } from "vite-plus/test";
import { EnvironmentId } from "@t3tools/contracts";
import { createFleetHostStore } from "./store.ts";

const enrollment = {
  enrollmentId: "11111111-1111-4111-8111-111111111111",
  environmentId: EnvironmentId.make("host"),
  enabled: true,
  continueRunningThreads: false,
};
async function fixture(run: (home: string) => Promise<void>) {
  const home = await NodeFSP.mkdtemp(
    NodePath.join(NodeOS.tmpdir(), "jones-fleet-host-store-test-"),
  );
  try {
    await run(home);
  } finally {
    await NodeFSP.rm(home, { recursive: true, force: true });
  }
}
describe("fleet host receipt storage", () => {
  it("retains enrollment across store recreation outside userdata", async () =>
    fixture(async (home) => {
      await createFleetHostStore(home).editEnrollment(() => enrollment);
      expect(await createFleetHostStore(home).readEnrollment()).toEqual(enrollment);
      expect(
        JSON.parse(
          await NodeFSP.readFile(
            NodePath.join(home, "runtime/jones-fleet/enrollment.json"),
            "utf8",
          ),
        ),
      ).toEqual(enrollment);
    }));
  it("preserves corrupt records instead of replacing them", async () =>
    fixture(async (home) => {
      const store = createFleetHostStore(home);
      await store.editEnrollment(() => enrollment);
      const path = NodePath.join(home, "runtime/jones-fleet/enrollment.json");
      await NodeFSP.writeFile(path, "corrupt retained evidence", { mode: 0o600 });
      await expect(
        store.editEnrollment(() => ({ ...enrollment, enabled: false })),
      ).rejects.toThrow();
      expect(await NodeFSP.readFile(path, "utf8")).toBe("corrupt retained evidence");
    }));
  it("rejects receipt symlinks and malformed operation paths", async () =>
    fixture(async (home) => {
      const store = createFleetHostStore(home);
      await store.readEnrollment();
      const target = NodePath.join(home, "owned-fixture.json");
      await NodeFSP.writeFile(target, JSON.stringify(enrollment), { mode: 0o600 });
      await NodeFSP.symlink(target, NodePath.join(home, "runtime/jones-fleet/enrollment.json"));
      await expect(store.readEnrollment()).rejects.toThrow();
      await expect(store.readOperation("../outside")).rejects.toThrow("Invalid fleet operation");
      expect(JSON.parse(await NodeFSP.readFile(target, "utf8"))).toEqual(enrollment);
    }));
});
