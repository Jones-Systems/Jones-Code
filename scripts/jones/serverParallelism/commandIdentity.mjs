import * as NodeCrypto from "node:crypto";

// Match currentBuildIdentity's SEA entry-file framing, including the newline.
export function executableIdentity(bytes) {
  const sha256 = NodeCrypto.createHash("sha256").update(bytes).digest("hex");
  return NodeCrypto.createHash("sha256")
    .update(JSON.stringify({ path: "entry", sha256 }) + "\n")
    .digest("hex");
}

export function stampedIdentity(bytes) {
  return /jones-server-compute-build:([a-f0-9]{64})/.exec(bytes.toString("utf8"))?.[1];
}
