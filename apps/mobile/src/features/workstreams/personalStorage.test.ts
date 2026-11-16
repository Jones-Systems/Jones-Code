import { afterEach, expect, it, vi } from "vite-plus/test";
import { personalMobileStorage } from "./personalStorage";

const fixture = vi.hoisted(() => ({
  files: new Map<string, string>(),
  write: vi.fn<(file: { uri: string }, value: string) => Promise<void>>(),
}));
vi.mock("../../lib/atomic-file", () => ({ writeFileAtomically: fixture.write }));
vi.mock("expo-file-system", () => ({
  Paths: { document: "synthetic://documents" },
  Directory: class {
    uri: string;
    constructor(parent: string, name: string) {
      this.uri = `${parent}/${name}`;
    }
    create() {}
  },
  File: class {
    uri: string;
    constructor(parent: { uri: string }, name: string) {
      this.uri = `${parent.uri}/${name}`;
    }
    get exists() {
      return fixture.files.has(this.uri);
    }
    async text() {
      return fixture.files.get(this.uri)!;
    }
  },
}));
afterEach(() => {
  fixture.files.clear();
  fixture.write.mockReset();
});

it("keeps the newest cached observation when a native write is delayed", async () => {
  let release!: () => void;
  const delayed = new Promise<void>((resolve) => {
    release = resolve;
  });
  fixture.write.mockImplementationOnce(async (file, value) => {
    await delayed;
    fixture.files.set(file.uri, value);
  });
  fixture.write.mockImplementation(async (file, value) => {
    fixture.files.set(file.uri, value);
  });
  const first = personalMobileStorage.setItem("cache:synthetic", "older");
  const second = personalMobileStorage.setItem("cache:synthetic", "newer");
  const read = personalMobileStorage.getItem("cache:synthetic");
  await vi.waitFor(() => expect(fixture.write).toHaveBeenCalledTimes(1));
  release();
  await Promise.all([first, second]);
  expect(await read).toBe("newer");
});

it("reports a failed persistence attempt and permits a subsequent save", async () => {
  fixture.write.mockRejectedValueOnce(new Error("Synthetic storage failure"));
  await expect(personalMobileStorage.setItem("origin", "first")).rejects.toThrow(
    "Synthetic storage failure",
  );
  fixture.write.mockImplementation(async (file, value) => {
    fixture.files.set(file.uri, value);
  });
  await personalMobileStorage.setItem("origin", "retry");
  expect(await personalMobileStorage.getItem("origin")).toBe("retry");
  expect(await personalMobileStorage.getItem("absent")).toBeNull();
});
