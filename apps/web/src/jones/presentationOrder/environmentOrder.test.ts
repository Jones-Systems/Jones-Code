import { expect, it } from "vite-plus/test";
import { sortEnvironmentOptions } from "./environmentOrder";
import { mergeVisibleOrder } from "./projectOrder";
const local = { environmentId: "local", label: "Zulu", isPrimary: true };
const remote = { environmentId: "remote", label: "Alpha", isPrimary: false };
it("uses primary then label before ordering and lets manual order outrank primary", () => {
  expect(sortEnvironmentOptions([remote, local], [])).toEqual([local, remote]);
  expect(sortEnvironmentOptions([local, remote], ["remote", "local"])).toEqual([remote, local]);
});
it("keeps stable IDs through rename and appends new or re-paired environments", () => {
  const renamed = { ...remote, label: "Renamed" };
  const repaired = { ...remote, environmentId: "new-remote" };
  expect(
    sortEnvironmentOptions([repaired, local, renamed], ["remote", "local"]).map(
      (item) => item.environmentId,
    ),
  ).toEqual(["remote", "local", "new-remote"]);
});
it("preserves disconnected slots and returns to the current primary on reset", () => {
  expect(mergeVisibleOrder(["local", "offline", "remote"], ["remote", "local"])).toEqual([
    "remote",
    "offline",
    "local",
  ]);
  expect(
    sortEnvironmentOptions(
      [
        { ...local, isPrimary: false },
        { ...remote, isPrimary: true },
      ],
      [],
    )[0]!.environmentId,
  ).toBe("remote");
});
