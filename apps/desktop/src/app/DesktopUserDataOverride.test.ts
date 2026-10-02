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
