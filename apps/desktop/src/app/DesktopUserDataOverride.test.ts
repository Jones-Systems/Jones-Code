// @effect-diagnostics nodeBuiltinImport:off - Bootstrap tests exercise POSIX and Windows path rules before an Effect runtime.
import * as NodePath from "node:path";
import { assert, describe, it } from "vite-plus/test";
import {
  configureDesktopUserDataOverride,
  resolveDesktopUserDataOverride,
} from "./DesktopUserDataOverride.ts";

describe("desktop client profile override", () => {
  it("leaves default paths untouched when no override is selected", () => {
    for (const directory of [undefined, "", "   "]) {
      configureDesktopUserDataOverride({
        directory,
        path: NodePath.posix,
        createDirectory: () => assert.fail("default profile must not be created here"),
        setPath: () => assert.fail("default profile must not be rebound here"),
      });
    }
  });

  it("creates and binds both storage paths synchronously in order", () => {
    const operations: string[] = [];
    configureDesktopUserDataOverride({
      directory: " /isolated/other/../profile ",
      path: NodePath.posix,
      createDirectory: (directory) => operations.push(`mkdir:${directory}`),
      setPath: (name, directory) => operations.push(`${name}:${directory}`),
    });
    assert.deepEqual(operations, [
      "mkdir:/isolated/profile",
      "userData:/isolated/profile",
      "sessionData:/isolated/profile",
    ]);
  });

  it("rejects relative paths before any filesystem or Electron effect", () => {
    assert.throws(
      () =>
        configureDesktopUserDataOverride({
          directory: "relative/profile",
          path: NodePath.posix,
          createDirectory: () => assert.fail("invalid path must not touch disk"),
          setPath: () => assert.fail("invalid path must not bind storage"),
        }),
      /must be an absolute path/,
    );
  });

  it("accepts absolute paths using the receiving platform's path rules", () => {
    assert.equal(
      resolveDesktopUserDataOverride("C:\\isolated\\profile", NodePath.win32),
      "C:\\isolated\\profile",
    );
  });
});

describe("profile path validation and failure ordering", () => {
  it.each([
    { path: NodePath.posix, directory: "relative/profile" },
    { path: NodePath.win32, directory: "C:relative-profile" },
    { path: NodePath.posix, directory: "/profile\0invalid" },
    { path: NodePath.win32, directory: "C:\\profile\0invalid" },
  ])("rejects invalid profile $directory before effects", ({ path, directory }) => {
    const effects: string[] = [];
    assert.throws(() =>
      configureDesktopUserDataOverride({
        path,
        directory,
        createDirectory: () => effects.push("create"),
        setPath: () => effects.push("bind"),
      }),
    );
    assert.deepEqual(effects, []);
  });

  it("preserves native path case and handles Windows UNC paths", () => {
    assert.equal(
      resolveDesktopUserDataOverride("/Profiles/MixedCase", NodePath.posix),
      "/Profiles/MixedCase",
    );
    assert.equal(
      resolveDesktopUserDataOverride("C:\\Profiles\\MixedCase", NodePath.win32),
      "C:\\Profiles\\MixedCase",
    );
    assert.equal(
      resolveDesktopUserDataOverride("\\\\Host\\Share\\Profile", NodePath.win32),
      "\\\\Host\\Share\\Profile",
    );
  });

  it("does not bind storage if directory creation fails", () => {
    const failure = new Error("synthetic mkdir failure");
    const effects: string[] = [];
    let caught: unknown;
    try {
      configureDesktopUserDataOverride({
        directory: "/isolated/profile",
        path: NodePath.posix,
        createDirectory: () => {
          throw failure;
        },
        setPath: () => effects.push("bind"),
      });
    } catch (error) {
      caught = error;
    }
    assert.strictEqual(caught, failure);
    assert.deepEqual(effects, []);
  });
});
