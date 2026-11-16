// @effect-diagnostics nodeBuiltinImport:off -- Simulate the native subprocess byte streams at their owning boundary.
import * as NodeEvents from "node:events";
import * as NodeStream from "node:stream";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";
const mocks = vi.hoisted(() => ({ spawn: vi.fn() }));
vi.mock("node:child_process", () => ({ spawn: mocks.spawn }));
import { runRawGit } from "./rawGit.ts";

function fakeChild() {
  const events = new NodeEvents.EventEmitter();
  const child = Object.assign(events, {
    stdout: new NodeStream.PassThrough(),
    stderr: new NodeStream.PassThrough(),
    pid: 123456,
    kill: vi.fn((_signal: string) => {
      queueMicrotask(() => events.emit("close", null, "SIGKILL"));
      return true;
    }),
  });
  mocks.spawn.mockReturnValue(child);
  return child;
}
afterEach(() => {
  vi.useRealTimers();
  mocks.spawn.mockReset();
});

describe("bounded raw Git subprocess lifecycle", () => {
  it("returns raw bytes and passes only fixed Git flags without a shell", async () => {
    const child = fakeChild();
    const promise = runRawGit(
      "/synthetic",
      ["cat-file", "blob", "a".repeat(40)],
      1024,
      new AbortController().signal,
    );
    child.stdout.write(Buffer.from([0xff, 0, 0x0d, 0x0a]));
    child.emit("close", 0);
    const result = await promise;
    expect([...result.stdout]).toEqual([0xff, 0, 0x0d, 0x0a]);
    const [command, args, options] = mocks.spawn.mock.calls[0]!;
    expect(command).toBe("git");
    expect(options.shell).toBe(false);
    expect(args).toContain("--literal-pathspecs");
    expect(args).toContain("--no-replace-objects");
    expect(child.kill).not.toHaveBeenCalled();
  });
  it.each(["stdout", "stderr"] as const)(
    "kills only its captured child on %s overflow and returns no partial output",
    async (stream) => {
      const child = fakeChild();
      const promise = runRawGit(
        "/synthetic",
        ["rev-parse", "--show-toplevel"],
        2,
        new AbortController().signal,
      );
      const assertion = expect(promise).rejects.toMatchObject({ code: "git_output_overflow" });
      child[stream].write(Buffer.alloc(stream === "stdout" ? 3 : 8193));
      await assertion;
      expect(child.kill).toHaveBeenCalledWith("SIGKILL");
    },
  );
  it("enforces the ten-second deadline and waits for its child's close", async () => {
    vi.useFakeTimers();
    const child = fakeChild();
    const promise = runRawGit(
      "/synthetic",
      ["rev-parse", "--show-toplevel"],
      1024,
      new AbortController().signal,
    );
    const assertion = expect(promise).rejects.toMatchObject({ code: "git_timeout" });
    await vi.advanceTimersByTimeAsync(9999);
    expect(child.kill).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);
    await assertion;
    expect(child.kill).toHaveBeenCalledWith("SIGKILL");
  });
  it("cancels its captured child and does not spawn for an already cancelled call", async () => {
    const child = fakeChild();
    const controller = new AbortController();
    const promise = runRawGit(
      "/synthetic",
      ["rev-parse", "--show-toplevel"],
      1024,
      controller.signal,
    );
    const assertion = expect(promise).rejects.toMatchObject({ code: "cancelled" });
    controller.abort();
    await assertion;
    expect(child.kill).toHaveBeenCalledWith("SIGKILL");
    const spawns = mocks.spawn.mock.calls.length;
    await expect(
      runRawGit("/synthetic", ["rev-parse"], 1024, controller.signal),
    ).rejects.toMatchObject({ code: "cancelled" });
    expect(mocks.spawn).toHaveBeenCalledTimes(spawns);
  });
  it("reports unconfirmed cleanup rather than hanging or claiming that the child stopped", async () => {
    vi.useFakeTimers();
    const child = fakeChild();
    child.kill.mockImplementation(() => false);
    const promise = runRawGit("/synthetic", ["rev-parse"], 1024, new AbortController().signal);
    const assertion = expect(promise).rejects.toMatchObject({ code: "git_cleanup_failed" });
    await vi.advanceTimersByTimeAsync(11_000);
    await assertion;
    expect(child.kill).toHaveBeenCalledWith("SIGKILL");
    child.emit("close", 0);
  });
  it("refuses spawn failures without returning diagnostics or config output", async () => {
    const child = fakeChild();
    const promise = runRawGit("/synthetic", ["rev-parse"], 1024, new AbortController().signal);
    const assertion = expect(promise).rejects.toMatchObject({ code: "git_unavailable" });
    child.stderr.write("https://secret@private.example/private/config");
    child.emit("error", new Error("private environment"));
    child.emit("close", null);
    await assertion;
    try {
      await promise;
    } catch (error) {
      expect(String(error)).not.toContain("secret");
      expect(String(error)).not.toContain("private");
    }
  });
});
