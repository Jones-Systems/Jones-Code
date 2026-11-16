import { expect, it } from "vite-plus/test";
import { createPickerFixture, createPickerObservations } from "./packaged-picker-scenario.mjs";
import { createMemoryIpc } from "./packaged-picker-memory-ipc.mjs";

it("keeps synthetic catalog traffic finite and transport downloads forbidden", async () => {
  const fixture = createPickerFixture("1".repeat(40));
  await expect(fixture.transport.api("unexpected")).rejects.toThrow(
    "Unexpected synthetic endpoint",
  );
  await expect(fixture.transport.download(1, "unused")).rejects.toThrow(
    "Transport download forbidden",
  );
  expect(fixture.downloads).toHaveLength(1);
});
it("joins a named observation without polling", async () => {
  const observations = createPickerObservations();
  let value = 0;
  const pending = observations.wait(
    (value) => value === 1,
    async () => value,
  );
  value = 1;
  observations.notify();
  await expect(pending).resolves.toBe(1);
});
it("clones payloads and results and disposes the synthetic listener", async () => {
  const ipc = createMemoryIpc();
  let calls = 0;
  ipc.main.handle("fixture", (_event, raw) => raw);
  const value = { selected: 1 };
  const result = await ipc.renderer.invoke("fixture", value);
  expect(result).toEqual(value);
  expect(result).not.toBe(value);
  const listener = () => {
    calls++;
  };
  ipc.renderer.on("fixture", listener);
  await ipc.send("fixture", value);
  ipc.renderer.removeListener("fixture", listener);
  await ipc.send("fixture", value);
  expect(calls).toBe(1);
});

it("never reuses an immutable artifact ID across latest and repeated alternate rebuilds", () => {
  const fixture = createPickerFixture("1".repeat(40));
  const ids = new Set(fixture.builds.map((value) => (value as { artifactId: number }).artifactId));
  expect(ids.size).toBe(2);
  for (let rebuild = 0; rebuild < 3; rebuild++) {
    fixture.rebuild();
    const alternate = fixture.builds[1] as { artifactId: number };
    expect(ids.has(alternate.artifactId)).toBe(false);
    ids.add(alternate.artifactId);
  }
});
