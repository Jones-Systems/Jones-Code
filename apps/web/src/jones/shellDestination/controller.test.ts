import { describe, expect, it, vi } from "vite-plus/test";
import { ShellRunController, SHELL_RUN_THREAD_ID } from "./controller";
import {
  SHELL_RUN_STORAGE_KEY,
  SHELL_RUN_TTL,
  ShellRunStore,
  shellRunKey,
  type ShellRun,
} from "./runStore";
import type { ShellDestination } from "./model";

const now = 1_000_000_000;
const id = "00000000-0000-4000-8000-000000000001";
const key = shellRunKey("source", "thread", 42, "destination", "pwd\n");
const destination: ShellDestination = {
  status: "ready",
  environmentId: "destination",
  label: "Mini",
  cwd: "/destination/server-cwd",
  reason: null,
};
function setup(raw?: string) {
  const data = new Map<string, string>(raw ? [[SHELL_RUN_STORAGE_KEY, raw]] : []);
  const storage = {
    getItem: (key: string) => data.get(key) ?? null,
    setItem: (key: string, value: string) => {
      data.set(key, value);
    },
  };
  const store = new ShellRunStore(storage, () => now);
  const controller = new ShellRunController(
    store,
    () => id,
    () => now,
  );
  const terminal = {
    open: vi.fn(async () => true),
    write: vi.fn(async () => true),
    close: vi.fn(async () => true),
  };
  const input = { key, code: "pwd\n", complete: true, destination: () => destination, terminal };
  return { data, storage, store, controller, terminal, input };
}
function deferred() {
  let resolve!: (result: boolean) => void;
  const promise = new Promise<boolean>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

describe("native destination run controller", () => {
  it("records intent before exactly one destination-local open and one CR-terminated write", async () => {
    const { controller, input, store, terminal } = setup();
    terminal.open.mockImplementation(async () => {
      expect(store.get(key)?.phase).toBe("opening");
      return true;
    });
    terminal.write.mockImplementation(async () => {
      expect(store.get(key)?.phase).toBe("writing");
      return true;
    });
    await controller.run(input);
    expect(terminal.open).toHaveBeenCalledExactlyOnceWith({
      environmentId: "destination",
      input: {
        threadId: SHELL_RUN_THREAD_ID,
        terminalId: `run-${id}`,
        cwd: "/destination/server-cwd",
      },
    });
    expect(terminal.write).toHaveBeenCalledExactlyOnceWith({
      environmentId: "destination",
      input: { threadId: SHELL_RUN_THREAD_ID, terminalId: `run-${id}`, data: "pwd\r" },
    });
    expect(store.get(key)?.phase).toBe("running");
    expect(terminal.close).not.toHaveBeenCalled();
  });
  it("blocks synchronous double clicks and focuses/reuses an existing run without resend", async () => {
    const { controller, input, terminal } = setup();
    const opening = deferred();
    terminal.open.mockReturnValue(opening.promise);
    const first = controller.run(input);
    await controller.run(input);
    opening.resolve(true);
    await first;
    await controller.run(input);
    expect(terminal.open).toHaveBeenCalledTimes(1);
    expect(terminal.write).toHaveBeenCalledTimes(1);
  });
  it("continues an accepted pending run after the view unsubscribes", async () => {
    const { controller, input, terminal, store } = setup();
    const opening = deferred();
    terminal.open.mockReturnValue(opening.promise);
    const unmount = store.subscribe(() => {});
    const pending = controller.run(input);
    unmount();
    opening.resolve(true);
    await pending;
    expect(terminal.write).toHaveBeenCalledTimes(1);
    expect(terminal.close).not.toHaveBeenCalled();
  });
  it.each(["opening", "writing"] as const)(
    "does not resend a run restored during %s",
    async (phase) => {
      const { controller, input, terminal, store } = setup(
        JSON.stringify([
          {
            key,
            environmentId: "destination",
            terminalId: `run-${id}`,
            cwd: "/destination",
            phase,
            updatedAt: now,
          },
        ]),
      );
      await controller.run(input);
      expect(store.get(key)?.phase).toBe("writeUnknown");
      expect(terminal.open).not.toHaveBeenCalled();
      expect(terminal.write).not.toHaveBeenCalled();
    },
  );
  it("does not write if recording the write intent fails after open", async () => {
    const { storage, store, controller, input, terminal } = setup();
    const save = storage.setItem;
    storage.setItem = (key, value) => {
      if (value.includes('"writing"')) throw new Error("quota");
      save(key, value);
    };
    await controller.run(input);
    expect(terminal.open).toHaveBeenCalledTimes(1);
    expect(terminal.write).not.toHaveBeenCalled();
    expect(store.get(key)?.phase).toBe("writeUnknown");
  });
  it.each([
    "invalid",
    "self",
    "connecting",
    "offline",
    "no-permission",
    "unknown",
    "label-mismatch",
  ] as const)("never opens for %s", async (status) => {
    const { controller, input, terminal } = setup();
    await controller.run({ ...input, destination: () => ({ ...destination, status }) });
    expect(terminal.open).not.toHaveBeenCalled();
    expect(terminal.write).not.toHaveBeenCalled();
  });
  it("rechecks destination permission after open", async () => {
    const { controller, input, terminal, store } = setup();
    const read = vi
      .fn()
      .mockReturnValueOnce(destination)
      .mockReturnValue({ ...destination, status: "no-permission" });
    await controller.run({ ...input, destination: read });
    expect(terminal.write).not.toHaveBeenCalled();
    expect(store.get(key)?.phase).toBe("writeUnknown");
  });
  it.each(["open", "write"] as const)(
    "keeps unknown delivery after interrupted %s without retry",
    async (operation) => {
      const { controller, input, terminal, store, storage } = setup();
      terminal[operation].mockRejectedValue(new Error("interrupted"));
      await controller.run(input);
      expect(store.get(key)?.phase).toBe("writeUnknown");
      const restored = new ShellRunController(
        new ShellRunStore(storage, () => now),
        () => id,
        () => now,
      );
      await restored.run(input);
      expect(terminal.open).toHaveBeenCalledTimes(1);
      expect(terminal.write).toHaveBeenCalledTimes(operation === "open" ? 0 : 1);
    },
  );
  it("survives view unsubscribe/navigation and only explicit Close deletes history", async () => {
    const { controller, input, terminal, store } = setup();
    const unmount = store.subscribe(() => {});
    await controller.run(input);
    unmount();
    expect(terminal.close).not.toHaveBeenCalled();
    expect(await controller.close(key, terminal, () => false)).toBe(false);
    expect(await controller.close(key, terminal, () => true)).toBe(true);
    expect(terminal.close).toHaveBeenCalledExactlyOnceWith({
      environmentId: "destination",
      input: { threadId: SHELL_RUN_THREAD_ID, terminalId: `run-${id}`, deleteHistory: true },
    });
    expect(store.get(key)?.phase).toBe("closed");
  });
  it("reconciles missing/restarted and exited sessions without replaying", async () => {
    const { controller, input, terminal, store } = setup();
    await controller.run(input);
    controller.reconcile(key, null);
    expect(store.get(key)?.phase).toBe("running");
    controller.reconcile(key, [{ terminalId: `run-${id}`, status: "exited" }]);
    expect(store.get(key)?.phase).toBe("exited");
    controller.reconcile(key, []);
    expect(store.get(key)?.phase).toBe("unavailable");
    await controller.run(input);
    expect(terminal.write).toHaveBeenCalledTimes(1);
  });
  it("Run again explicitly allocates a fresh terminal", async () => {
    const { controller, input, store, terminal } = setup();
    await controller.run(input);
    const next = new ShellRunController(
      store,
      () => "00000000-0000-4000-8000-000000000002",
      () => now,
    );
    await next.run({ ...input, again: true });
    expect(store.get(key)?.terminalId).toBe("run-00000000-0000-4000-8000-000000000002");
    expect(terminal.open).toHaveBeenCalledTimes(2);
    expect(terminal.write).toHaveBeenCalledTimes(2);
    expect(terminal.close).not.toHaveBeenCalled();
  });
  it("fails closed when durable intent cannot be saved", async () => {
    const { terminal, input } = setup();
    const controller = new ShellRunController(new ShellRunStore(null), () => id);
    await expect(controller.run(input)).rejects.toThrow("storage");
    expect(terminal.open).not.toHaveBeenCalled();
  });
});

describe("bounded local run associations", () => {
  const run: ShellRun = {
    key,
    environmentId: "destination",
    terminalId: `run-${id}`,
    cwd: "/destination",
    phase: "running",
    updatedAt: now,
  };
  it.each(["opening", "writing"] as const)(
    "restores %s as unknown without retaining command/input",
    (phase) => {
      const { store, storage } = setup(
        JSON.stringify([{ ...run, phase, command: "private input", password: "private input" }]),
      );
      expect(store.get(key)?.phase).toBe("writeUnknown");
      store.mark(key, "writeUnknown");
      expect(storage.getItem(SHELL_RUN_STORAGE_KEY)).not.toContain("private input");
    },
  );
  it("rejects invalid records, expires old associations, and caps at 32", () => {
    const { store, storage } = setup(
      JSON.stringify([
        { ...run, terminalId: "invalid" },
        { ...run, updatedAt: now - SHELL_RUN_TTL },
      ]),
    );
    expect(store.get(key)).toBeNull();
    for (let index = 0; index < 40; index++)
      store.save({ ...run, key: shellRunKey("source", "thread", index, "destination", "pwd\n") });
    expect(JSON.parse(storage.getItem(SHELL_RUN_STORAGE_KEY)!)).toHaveLength(32);
  });
  it("separates source, thread, position, destination and command without persisting their contents", () => {
    const values = [
      shellRunKey("other", "thread", 42, "destination", "pwd\n"),
      shellRunKey("source", "other", 42, "destination", "pwd\n"),
      shellRunKey("source", "thread", 43, "destination", "pwd\n"),
      shellRunKey("source", "thread", 42, "other", "pwd\n"),
      shellRunKey("source", "thread", 42, "destination", "whoami\n"),
    ];
    expect(new Set([key, ...values]).size).toBe(6);
    expect(key).toMatch(/^[a-f0-9]{64}$/);
  });
});
