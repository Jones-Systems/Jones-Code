import * as NodeAssert from "node:assert/strict";
import * as NodeTest from "node:test";
import { startMacResourceSampler } from "./macos-resource-sampler.mjs";

const fixture = `
  const readline = require("node:readline");
  const buildRow = (pid, birth) => [pid, 15_000_000, 16_384, 1, birth, 1].join(":");
  let round = 0;
  console.log("ready");
  readline.createInterface({input:process.stdin}).on("line", (line) => {
    round++;
    const rows = line.split(" ").map((pid, index) => index === 0
      ? buildRow(pid, process.argv[1] === "reuse" ? 100 + round : 100)
      : pid + ":unavailable");
    rows.push(buildRow(process.pid, 100));
    if (process.argv[1] === "bad-pid") rows[0] = buildRow(process.pid, 100);
    console.log("rows " + rows.join(" "));
  });
`;

NodeTest.test("preserves missing workers and accounts for the observer separately", async () => {
  const sampler = await startMacResourceSampler(process.execPath, ["--eval", fixture]);
  try {
    const missing = 2_147_483_647;
    const result = await sampler.sample([process.pid, missing]);
    NodeAssert.equal(result.status, "partial");
    NodeAssert.deepEqual(result.missingPids, [missing]);
    NodeAssert.deepEqual(result.processes, [
      { pid: process.pid, cpuMs: 15, rssBytes: 16_384, threads: 1 },
    ]);
    NodeAssert.deepEqual(sampler.observer(), {
      method: "macos-proc-info",
      pid: sampler.observer().pid,
      samples: 1,
      maxRssBytes: 16_384,
      maxThreads: 1,
      cpuMs: 15,
    });
  } finally {
    NodeAssert.deepEqual(await sampler.close(), { capturedExit: true, code: 0, signal: null });
  }
});

NodeTest.test("rejects a reply for the wrong PID", async () => {
  const sampler = await startMacResourceSampler(process.execPath, ["--eval", fixture, "bad-pid"]);
  try {
    await NodeAssert.rejects(sampler.sample([process.pid]), /Invalid sampler PID/);
  } finally {
    NodeAssert.equal((await sampler.close()).capturedExit, true);
  }
});

NodeTest.test("rejects a PID when the recorded process start time changes", async () => {
  const sampler = await startMacResourceSampler(process.execPath, ["--eval", fixture, "reuse"]);
  try {
    NodeAssert.equal((await sampler.sample([process.pid])).status, "complete");
    await NodeAssert.rejects(sampler.sample([process.pid]), /process identity changed/);
  } finally {
    NodeAssert.equal((await sampler.close()).capturedExit, true);
  }
});
