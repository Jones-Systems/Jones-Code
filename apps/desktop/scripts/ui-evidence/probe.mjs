import * as NodeFSP from "node:fs/promises";
import * as NodeNet from "node:net";
import * as NodePath from "node:path";
import { sandboxEnvironment } from "./sandbox.mjs";

export async function isolationProbe(realHome) {
  const absent = [];
  for (const name of [".t3", ".config", ".claude", ".codex", ".ssh"]) {
    const candidate = NodePath.join(realHome, name);
    let visible = false;
    try {
      await NodeFSP.access(candidate);
      visible = true;
    } catch (error) {
      if (!["ENOENT", "EACCES"].includes(error.code)) throw error;
    }
    if (visible) throw new Error(`Isolation probe found live home path: ${name}`);
    absent.push(name);
  }
  const interfaces = (await NodeFSP.readFile("/proc/net/dev", "utf8"))
    .split("\n")
    .slice(2)
    .filter((line) => line.includes(":"))
    .map((line) => line.split(":")[0].trim())
    .sort();
  if (interfaces.join(",") !== "lo")
    throw new Error("Isolation probe found a non-loopback interface");
  const expected = sandboxEnvironment();
  if (
    Object.keys(process.env).sort().join(",") !== Object.keys(expected).sort().join(",") ||
    Object.entries(expected).some(([key, value]) => process.env[key] !== value)
  )
    throw new Error("Isolation environment differs from allowlist");
  const refused = await new Promise((resolve, reject) => {
    const socket = NodeNet.connect({ host: "127.0.0.1", port: 3773 });
    socket.once("connect", () => {
      socket.destroy();
      resolve(false);
    });
    socket.once("error", (error) => {
      socket.destroy();
      if (error.code === "ECONNREFUSED") resolve(true);
      else reject(error);
    });
    socket.setTimeout(2000, () => {
      socket.destroy();
      reject(new Error("Live endpoint isolation probe timed out"));
    });
  });
  if (!refused) throw new Error("Isolation probe reached the live endpoint");
  return {
    status: "passed",
    liveHomeAbsent: absent,
    liveEndpointRefused: refused,
    interfaces,
    environmentNames: Object.keys(expected).sort(),
  };
}
