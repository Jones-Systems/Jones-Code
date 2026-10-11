import { mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
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
  const home = await mkdtemp(join(tmpdir(), "jones-fleet-host-store-test-"));
  try {
    await run(home);
  } finally {
    await rm(home, { recursive: true, force: true });
  }
}
describe("fleet host receipt storage", () => {
  it("retains enrollment across store recreation outside userdata", async () =>
    fixture(async (home) => {
      await createFleetHostStore(home).editEnrollment(() => enrollment);
      expect(await createFleetHostStore(home).readEnrollment()).toEqual(enrollment);
      expect(
        JSON.parse(await readFile(join(home, "runtime/jones-fleet/enrollment.json"), "utf8")),
      ).toEqual(enrollment);
    }));
  it("preserves corrupt records instead of replacing them", async () =>
    fixture(async (home) => {
      const store = createFleetHostStore(home);
      await store.editEnrollment(() => enrollment);
      const path = join(home, "runtime/jones-fleet/enrollment.json");
      await writeFile(path, "corrupt retained evidence", { mode: 0o600 });
      await expect(
        store.editEnrollment(() => ({ ...enrollment, enabled: false })),
      ).rejects.toThrow();
      expect(await readFile(path, "utf8")).toBe("corrupt retained evidence");
    }));
  it("rejects receipt symlinks and malformed operation paths", async () =>
    fixture(async (home) => {
      const store = createFleetHostStore(home);
      await store.readEnrollment();
      const target = join(home, "owned-fixture.json");
      await writeFile(target, JSON.stringify(enrollment), { mode: 0o600 });
      await symlink(target, join(home, "runtime/jones-fleet/enrollment.json"));
      await expect(store.readEnrollment()).rejects.toThrow();
      await expect(store.readOperation("../outside")).rejects.toThrow("Invalid fleet operation");
      expect(JSON.parse(await readFile(target, "utf8"))).toEqual(enrollment);
    }));
});
