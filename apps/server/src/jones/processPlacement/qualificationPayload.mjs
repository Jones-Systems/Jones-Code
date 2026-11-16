import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeReadline from "node:readline";
import * as NodeURL from "node:url";

const [mode, sentinel, ...args] = process.argv.slice(2);
if (mode === "sentinel") {
  NodeFS.writeFileSync(sentinel, "UNEXPECTED_PAYLOAD_EXECUTION", { flag: "wx" });
  process.stdout.write("UNEXPECTED_PAYLOAD_EXECUTION\n");
  process.exit(99);
}
const membership = NodeFS.readFileSync("/proc/self/cgroup", "utf8");
// Every owned process has a deadline, including when the runner disappears.
let expire = () => process.exit(124);
const deadline = setTimeout(() => expire(), 15_000);
if (mode === "descendant") {
  process.on("disconnect", () => process.exit(0));
  process.on("SIGTERM", () => process.exit(0));
  process.send({ pid: process.pid, membership });
} else {
  NodeFS.writeFileSync(sentinel, String(process.pid), { flag: "wx" });
  const child = NodeChildProcess.fork(NodeURL.fileURLToPath(import.meta.url), ["descendant"], {
    stdio: ["ignore", "ignore", "ignore", "ipc"],
  });
  let stopping = false;
  const closed = new Promise((resolve) => child.once("close", resolve));
  async function stop(code) {
    if (stopping) return;
    stopping = true;
    child.kill("SIGTERM");
    await closed;
    NodeFS.writeFileSync(`${sentinel}.reaped`, String(child.pid));
    clearTimeout(deadline);
    process.exit(code);
  }
  expire = () => void stop(124);
  process.on("SIGTERM", () => void stop(0));
  process.on("disconnect", () => void stop(0));
  child.once("error", () => void stop(123));
  child.once("message", (descendant) => {
    const receipt = {
      pid: process.pid,
      membership,
      descendant,
      args,
      cwd: process.cwd(),
      environment: process.env.QUALIFICATION_VALUE,
    };
    process.stdout.write(`${JSON.stringify(receipt)}\n`);
    process.stderr.write("qualification-stderr\n");
    process.send?.({ ready: receipt });
  });
  process.on("message", (message) => process.send?.({ echo: message }));
  const lines = NodeReadline.createInterface({ input: process.stdin });
  lines.on("line", (line) => {
    if (line === "exit:23") void stop(23);
    else process.stdout.write(`stdin:${line}\n`);
  });
  lines.on("close", () => void stop(0));
}
