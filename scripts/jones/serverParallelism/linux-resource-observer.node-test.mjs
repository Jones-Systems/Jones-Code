import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import * as NodeURL from "node:url";
import * as HostProcess from "../../../packages/shared/src/HostProcess.ts";
import { startLinuxResourceObserver } from "./linux-resource-observer.mjs";

async function fixture(t, onMessage, timeoutMs) {
  const directory = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "linux-observer-test-"));
  const module = NodePath.join(directory, "fixture.mjs");
  await NodeFSP.writeFile(
    module,
    `process.on("disconnect", () => process.exit());
process.on("message", ${onMessage});
process.send({ id: 0, ready: true });`,
    { mode: 0o600, flag: "wx" },
  );
  t.after(async () => {
    await NodeFSP.unlink(module);
    await NodeFSP.rmdir(directory);
  });
  const observer = await startLinuxResourceObserver({
    modulePath: NodeURL.pathToFileURL(module).href,
    timeoutMs,
  });
  t.after(() => observer.close());
  return observer;
}

NodeTest.test(
  "the observer binds requests and keeps capture tracking attached to the IPC read",
  async (t) => {
    const observer = await fixture(
      t,
      `({ id, pids }) => process.send({
      id,
      sample: { requestedPids: pids, processes: [], missingPids: pids, errors: [], status: "unavailable" },
      usage: { cpuMs: 1, maxRssBytes: 1024, threads: 7 },
    })`,
    );
    await NodeAssert.rejects(observer.sample([process.pid, process.pid]), /Invalid captured/);
    let tracked;
    const pending = observer.sample([process.pid], undefined, (read) => {
      tracked = read;
    });
    NodeAssert.equal(tracked, pending);
    NodeAssert.deepEqual(await pending, {
      requestedPids: [process.pid],
      processes: [],
      missingPids: [process.pid],
      errors: [],
      status: "unavailable",
    });
    const { pid, ...usage } = observer.observer();
    NodeAssert.ok(Number.isInteger(pid) && pid > 0);
    NodeAssert.deepEqual(usage, {
      samples: 1,
      cpuMs: 1,
      maxRssBytes: 1024,
      maxThreads: 7,
    });
    const closing = observer.close();
    NodeAssert.equal(observer.close(), closing);
    NodeAssert.deepEqual(await closing, { capturedExit: true, code: 0, signal: null });
  },
);

NodeTest.test("a stale reply invalidates the exchange and captures retirement", async (t) => {
  const observer = await fixture(t, "({ id }) => process.send({ id: id + 1 })");
  await NodeAssert.rejects(
    observer.sample([process.pid]),
    /Unexpected Linux resource observer reply/,
  );
  NodeAssert.equal((await observer.close()).capturedExit, true);
  NodeAssert.equal(observer.observer().samples, 0);
});

NodeTest.test(
  "a timed out read retires the observer before rejecting the tracked read",
  async (t) => {
    const observer = await fixture(t, "() => {}", 750);
    const { pid } = observer.observer();
    await NodeAssert.rejects(observer.sample([process.pid]), /observer timed out/);
    NodeAssert.deepEqual(await observer.close(), { capturedExit: true, code: 0, signal: null });
    NodeAssert.throws(() => process.kill(pid, 0), { code: "ESRCH" });
  },
);

NodeTest.test(
  "the real Linux observer returns the complete procfs row and retires",
  { skip: HostProcess.Platform.defaultValue() !== "linux" },
  async (t) => {
    const observer = await startLinuxResourceObserver();
    t.after(() => observer.close());
    const sample = await observer.sample([process.pid]);
    NodeAssert.equal(sample.status, "complete");
    NodeAssert.equal(sample.processes.length, 1);
    NodeAssert.equal(sample.processes[0].pid, process.pid);
    NodeAssert.ok(sample.processes[0].fdinfo.attempted > 0);
    NodeAssert.ok(sample.processes[0].rssBytes > 0);
    NodeAssert.ok(observer.observer().maxRssBytes > 0);
    NodeAssert.deepEqual(await observer.close(), { capturedExit: true, code: 0, signal: null });
  },
);
