import * as NodeFSP from "node:fs/promises";

export async function processIdentity(pid) {
  if (!pid) return null;
  try {
    const value = await NodeFSP.readFile(`/proc/${pid}/stat`, "utf8");
    return value.slice(value.lastIndexOf(")") + 2).split(" ")[19];
  } catch (error) {
    if (["ENOENT", "ESRCH"].includes(error.code)) return null;
    throw error;
  }
}
