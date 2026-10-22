import { describe, expect, it, vi } from "vite-plus/test";
import { ShellRunController, SHELL_RUN_THREAD_ID } from "./controller";
import {
  SHELL_RUN_STORAGE_KEY,
  SHELL_RUN_TTL,
  ShellRunStore,
  shellRunKey,
  type ShellRun,
  type ShellRunCoordination,
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
function tabCoordination(): ShellRunCoordination {
  const held = new Map<string, Promise<void>>();
  const exclusive: ShellRunCoordination["exclusive"] = async (name, work) => {
    const previous = held.get(name);
    let release!: () => void;
    const done = new Promise<void>((resolve) => {
      release = resolve;
    });
    held.set(name, done);
    await previous;
    try {
      return await work();
    } finally {
      if (held.get(name) === done) held.delete(name);
      release();
    }
  };
  return {
    exclusive,
    tryExclusive: async (name, work) => (held.has(name) ? undefined : exclusive(name, work)),
  };
}
function setup(raw?: string) {
  const data = new Map<string, string>(raw ? [[SHELL_RUN_STORAGE_KEY, raw]] : []);
  const storage = {
    getItem: (key: string) => data.get(key) ?? null,
    setItem: (key: string, value: string) => {
      data.set(key, value);
    },
  };
  const coordination = tabCoordination();
  const store = new ShellRunStore(storage, () => now, coordination);
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
  return { data, storage, store, controller, terminal, input, coordination };
}
function deferred() {
  let resolve!: (result: boolean) => void;
  const promise = new Promise<boolean>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

describe("native destination run controller", () => {
  it("preserves unknown delivery when a stale tab runs another block, including reload", async () => {
    const a = setup();
    const bStore = new ShellRunStore(a.storage, () => now, a.coordination);
    const b = new ShellRunController(
      bStore,
      () => "00000000-0000-4000-8000-000000000002",
      () => now,
    );
    a.terminal.write.mockResolvedValueOnce(false);
    await a.controller.run(a.input);
    await b.run({ ...a.input, key: shellRunKey("source", "thread", 43, "destination", "pwd\n") });
    const restored = new ShellRunStore(a.storage, () => now, a.coordination);
    expect(restored.get(key)?.phase).toBe("writeUnknown");
    await new ShellRunController(
      restored,
      () => id,
      () => now,
    ).run(a.input);
    expect(a.terminal.write).toHaveBeenCalledTimes(2);
  });
  it("admits exactly one ordinary concurrent Play across independent tabs", async () => {
    const a = setup();
    const b = new ShellRunController(
      new ShellRunStore(a.storage, () => now, a.coordination),
      () => "00000000-0000-4000-8000-000000000002",
      () => now,
    );
    await Promise.all([a.controller.run(a.input), b.run(a.input)]);
    expect(a.terminal.open).toHaveBeenCalledTimes(1);
    expect(a.terminal.write).toHaveBeenCalledTimes(1);
  });
  it("retains both tabs while one write acknowledgement is outstanding", async () => {
    const a = setup();
    const bStore = new ShellRunStore(a.storage, () => now, a.coordination);
    const b = new ShellRunController(
      bStore,
      () => "00000000-0000-4000-8000-000000000002",
      () => now,
    );
    const writing = deferred();
    const entered = deferred();
    a.terminal.write.mockImplementationOnce(async () => {
      entered.resolve(true);
      return writing.promise;
    });
    const pending = a.controller.run(a.input);
    await entered.promise;
    const otherKey = shellRunKey("source", "thread", 45, "destination", "pwd\n");
    await b.run({ ...a.input, key: otherKey });
    // Unrelated keys finish even while A holds its lifecycle lock.
    expect(bStore.get(otherKey)?.phase).toBe("running");
    writing.resolve(false);
    await pending;
    const reload = new ShellRunStore(a.storage, () => now, a.coordination);
    expect(reload.get(key)?.phase).toBe("writeUnknown");
    expect(reload.get(otherKey)?.phase).toBe("running");
    await b.run(a.input);
    expect(bStore.get(key)?.phase).toBe("writeUnknown");
    expect(a.terminal.write).toHaveBeenCalledTimes(2);
  });
  it("serializes Run again and rejects stale replacement, close, and exit callbacks", async () => {
    const a = setup();
    a.terminal.write.mockResolvedValueOnce(false);
    await a.controller.run(a.input);
    const bStore = new ShellRunStore(a.storage, () => now, a.coordination);
    const b = new ShellRunController(
      bStore,
      () => "00000000-0000-4000-8000-000000000002",
      () => now,
    );
    const oldId = bStore.get(key)!.terminalId;
    await Promise.all([
      b.run({ ...a.input, again: true, expectedTerminalId: oldId }),
      a.controller.run({ ...a.input, again: true, expectedTerminalId: oldId }),
    ]);
    expect(a.terminal.write).toHaveBeenCalledTimes(2);
    expect(bStore.get(key)?.terminalId).not.toBe(oldId);
    await a.controller.run({ ...a.input, again: true, expectedTerminalId: oldId });
    expect(await a.controller.close(key, a.terminal, () => true, oldId)).toBe(false);
    await a.store.mark(key, "exited", oldId);
    expect(a.store.get(key)?.phase).toBe("running");
    expect(a.terminal.close).not.toHaveBeenCalled();
    expect(a.terminal.write).toHaveBeenCalledTimes(2);
  });
  it("fails closed without cross-tab coordination or when its lock is denied", async () => {
    const a = setup();
    for (const coordination of [
      null,
      {
        exclusive: async () => {
          throw new Error("lock denied");
        },
        tryExclusive: async () => {
          throw new Error("lock denied");
        },
      },
    ] as const) {
      const controller = new ShellRunController(
        new ShellRunStore(a.storage, () => now, coordination),
        () => id,
        () => now,
      );
      await expect(controller.run(a.input)).rejects.toThrow();
    }
    expect(a.terminal.open).not.toHaveBeenCalled();
    expect(a.terminal.write).not.toHaveBeenCalled();
  });
  it("fails closed when shared storage becomes unreadable or malformed after tab startup", async () => {
    const a = setup();
    a.data.set(SHELL_RUN_STORAGE_KEY, "invalid JSON");
    await expect(a.controller.run(a.input)).rejects.toThrow();
    a.storage.getItem = () => {
      throw new Error("storage denied");
    };
    await expect(a.controller.run(a.input)).rejects.toThrow("storage denied");
    expect(a.terminal.open).not.toHaveBeenCalled();
  });
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
      const { controller, input, terminal, store, storage, coordination } = setup();
      terminal[operation].mockRejectedValue(new Error("interrupted"));
      await controller.run(input);
      expect(store.get(key)?.phase).toBe("writeUnknown");
      const restored = new ShellRunController(
        new ShellRunStore(storage, () => now, coordination),
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
    await controller.reconcile(key, null);
    expect(store.get(key)?.phase).toBe("running");
    await controller.reconcile(key, [{ terminalId: `run-${id}`, status: "exited" }]);
    expect(store.get(key)?.phase).toBe("exited");
    await controller.reconcile(key, []);
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
    const controller = new ShellRunController(
      new ShellRunStore(null, Date.now, tabCoordination()),
      () => id,
    );
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
    async (phase) => {
      const { store, storage } = setup(
        JSON.stringify([{ ...run, phase, command: "private input", password: "private input" }]),
      );
      expect(store.get(key)?.phase).toBe("writeUnknown");
      await store.mark(key, "writeUnknown");
      expect(storage.getItem(SHELL_RUN_STORAGE_KEY)).not.toContain("private input");
    },
  );
  it("rejects invalid records, expires old associations, and fails closed at 32 unclosed runs", async () => {
    const { store, storage } = setup(
      JSON.stringify([
        { ...run, terminalId: "invalid" },
        { ...run, updatedAt: now - SHELL_RUN_TTL },
      ]),
    );
    expect(store.get(key)).toBeNull();
    for (let index = 0; index < 32; index++)
      await store.save({
        ...run,
        key: shellRunKey("source", "thread", index, "destination", "pwd\n"),
      });
    await expect(store.save(run)).rejects.toThrow("full");
    expect(JSON.parse(storage.getItem(SHELL_RUN_STORAGE_KEY)!)).toHaveLength(32);
  });
  it("refreshes observers without unstable snapshots or losing unknown delivery", async () => {
    const a = setup();
    const b = new ShellRunStore(a.storage, () => now, a.coordination);
    const listener = vi.fn();
    b.subscribe(listener);
    a.terminal.write.mockResolvedValue(false);
    await a.controller.run(a.input);
    b.observe();
    expect(b.get(key)?.phase).toBe("writeUnknown");
    const snapshot = b.get(key);
    b.observe();
    expect(b.get(key)).toBe(snapshot);
    expect(listener).toHaveBeenCalledTimes(1);
    // A delayed exit callback from a stale tab cannot erase the delivery warning.
    await b.mark(key, "exited", snapshot!.terminalId);
    expect(b.get(key)?.phase).toBe("writeUnknown");
  });
  it("reclaims only closed slots at capacity, preserving unexpired unknown delivery", async () => {
    const a = setup();
    for (let index = 0; index < 32; index++)
      await a.store.save({
        ...run,
        phase: index === 0 ? "closed" : "writeUnknown",
        key: shellRunKey("source", "thread", index, "destination", "pwd\n"),
      });
    await a.store.save(run);
    const records: ShellRun[] = JSON.parse(a.storage.getItem(SHELL_RUN_STORAGE_KEY)!);
    expect(records).toHaveLength(32);
    expect(records.filter((record) => record.phase === "writeUnknown")).toHaveLength(31);
  });
  it("refuses oversized storage before losing existing unknown tracking", async () => {
    const a = setup();
    await a.store.save({ ...run, phase: "writeUnknown" });
    const longCwd = "\u0001".repeat(32_768);
    for (let index = 0; index < 6; index++)
      await a.store.save({
        ...run,
        cwd: longCwd,
        key: shellRunKey("source", "thread", index, "destination", "pwd\n"),
      });
    await expect(
      a.store.save({
        ...run,
        cwd: longCwd,
        key: shellRunKey("source", "thread", 7, "destination", "pwd\n"),
      }),
    ).rejects.toThrow("full");
    const reloaded = new ShellRunStore(a.storage, () => now, a.coordination);
    expect(reloaded.get(key)?.phase).toBe("writeUnknown");
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
