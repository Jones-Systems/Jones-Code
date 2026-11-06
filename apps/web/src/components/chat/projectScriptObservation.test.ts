import { describe, expect, it } from "vite-plus/test";
import { writeProjectScriptOnce } from "./useChatTerminalActions";

describe("explicit project-script terminal forwarding", () => {
  it("forwards one bounded descriptor and preserves history/command bytes and native result", () => {
    const calls: unknown[][] = [];
    const native = { status: "native-write-accepted" };
    const result = writeProjectScriptOnce(
      (...args) => {
        calls.push(args);
        return native;
      },
      "term-1",
      " npm run build\r",
      { projectId: "project", scriptId: "build" },
      () => "4a792973-8d01-4eb5-a710-e34c74f57c6b",
    );
    expect(result).toBe(native);
    expect(calls).toEqual([
      [
        "term-1",
        " npm run build\r",
        {
          projectId: "project",
          scriptId: "build",
          invocationId: "4a792973-8d01-4eb5-a710-e34c74f57c6b",
        },
      ],
    ]);
  });

  it("falls back only before issuing bytes; an uncertain write is never replayed", () => {
    const calls: unknown[][] = [];
    writeProjectScriptOnce(
      (...args) => calls.push(args),
      "term-1",
      "build\r",
      { projectId: "project", scriptId: "build" },
      () => {
        throw Error("metadata unavailable");
      },
    );
    expect(calls).toEqual([["term-1", "build\r", undefined]]);
    let entered = 0;
    const lost = Error("accepted bytes, lost response");
    expect(() =>
      writeProjectScriptOnce(
        () => {
          entered++;
          throw lost;
        },
        "term-1",
        "build\r",
        { projectId: "project", scriptId: "build" },
        () => "4a792973-8d01-4eb5-a710-e34c74f57c6b",
      ),
    ).toThrow(lost);
    expect(entered).toBe(1);
  });
});
