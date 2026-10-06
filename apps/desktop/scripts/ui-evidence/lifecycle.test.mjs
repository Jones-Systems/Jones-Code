import * as NodeTest from "node:test";
import * as NodeAssert from "node:assert/strict";
import * as NodeEvents from "node:events";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import {
  allocateRun,
  superviseRun,
  cleanupRegistered,
  removeRun,
  stateParent,
} from "./lifecycle.mjs";

async function fixture(fn) {
  const parent = stateParent();
  await NodeFSP.mkdir(parent, { recursive: true, mode: 0o700 });
  const root = await NodeFSP.mkdtemp(NodePath.join(parent, "lifecycle-test-"));
  try {
    await fn(root);
  } finally {
    await NodeFSP.rm(root, { recursive: true, force: true });
  }
}
function fakeChild() {
  const child = new NodeEvents.EventEmitter();
  child.pid = undefined;
  child.exitCode = null;
  child.signalCode = null;
  child.finish = (code, signal = null) => {
    child.exitCode = code;
    child.signalCode = signal;
    child.emit("close", code, signal);
  };
  child.kill = (signal) => {
    queueMicrotask(() => child.finish(null, signal));
    return true;
  };
  return child;
}
for (const entry of [
  { name: "success", code: 0 },
  { name: "failure", code: 7 },
  { name: "SIGINT", signal: "SIGINT", code: 130 },
  { name: "SIGTERM", signal: "SIGTERM", code: 143 },
]) {
  NodeTest.test(`${entry.name} removes only scratch and preserves artifacts and result`, () =>
    fixture(async (parent) => {
      const run = await allocateRun(parent);
      const artifacts = NodePath.join(parent, "artifacts");
      await NodeFSP.mkdir(artifacts);
      await NodeFSP.writeFile(NodePath.join(artifacts, "evidence"), "retained");
      const signals = new NodeEvents.EventEmitter();
      const result = await superviseRun({
        run,
        signals,
        start: () => {
          const child = fakeChild();
          setImmediate(() =>
            entry.signal ? signals.emit(entry.signal) : child.finish(entry.code),
          );
          return child;
        },
      });
      NodeAssert.equal(result.code, entry.code);
      NodeAssert.equal(result.cleanup.outcome, "complete");
      await NodeAssert.rejects(NodeFSP.access(run.root), { code: "ENOENT" });
      NodeAssert.equal(
        await NodeFSP.readFile(NodePath.join(artifacts, "evidence"), "utf8"),
        "retained",
      );
      NodeAssert.equal(signals.listenerCount("SIGINT") + signals.listenerCount("SIGTERM"), 0);
    }),
  );
}
NodeTest.test("cleanup failure changes success but preserves an earlier failure", () =>
  fixture(async (parent) => {
    for (const code of [0, 7]) {
      const run = await allocateRun(parent);
      const result = await superviseRun({
        run,
        signals: new NodeEvents.EventEmitter(),
        remove: async () => {
          throw new Error("injected");
        },
        start: () => {
          const child = fakeChild();
          setImmediate(() => child.finish(code));
          return child;
        },
      });
      NodeAssert.equal(result.code, code === 0 ? 1 : code);
      NodeAssert.equal(result.cleanup.outcome, "unknown");
      await removeRun(run);
    }
  }),
);
NodeTest.test("concurrent owners cannot remove each other and registry rejects escaped roots", () =>
  fixture(async (parent) => {
    const [first, second] = await Promise.all([allocateRun(parent), allocateRun(parent)]);
    await removeRun(first);
    await NodeFSP.access(second.root);
    const record = { ...second, root: NodePath.join(parent, "artifacts") };
    await NodeFSP.writeFile(second.registry, JSON.stringify(record));
    await NodeAssert.rejects(cleanupRegistered(second.id, parent), /ownership/);
    await removeRun(second);
    await NodeAssert.rejects(cleanupRegistered("unknown", parent), /Invalid run ID/);
  }),
);
