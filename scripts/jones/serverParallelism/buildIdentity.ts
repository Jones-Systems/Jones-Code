// @effect-diagnostics nodeBuiltinImport:off -- Build-time plugin, before the application runtime exists.
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { resolve, relative } from "node:path";
import type { Plugin } from "vite-plus";

// Same length as the final hex digest to preserve generated source-map columns.
export const computeBuildPlaceholder =
  "f07a6bb84e82d6c130f80e7f7f398ea97a36ca2a6b5209ff71e56f613f0507c1";

/** Bind every emitted chunk to actual bundled inputs and outputs, not Git HEAD.
 * Lock/patch/config bytes bind external native dependencies. Packagers still
 * qualify staged native bytes/platform separately; this is not a binary signature.
 */
export function serverComputeBuildIdentity(
  root: string,
  options: Readonly<Record<string, unknown>>,
): Plugin {
  return {
    name: "jones-server-compute-build-identity",
    async generateBundle(_output, bundle) {
      const hash = createHash("sha256");
      const add = (name: string, bytes: string | Uint8Array) => {
        hash.update(
          JSON.stringify([name, createHash("sha256").update(bytes).digest("hex")]) + "\n",
        );
      };
      add("options", JSON.stringify(options));
      add("node", process.versions.node);
      for (const file of [
        "package.json",
        "pnpm-lock.yaml",
        "pnpm-workspace.yaml",
        "vite.config.ts",
        "apps/server/package.json",
        "apps/server/vite.config.ts",
        "patches/@ff-labs__fff-node@0.11.0.patch",
      ]) {
        add(file, await readFile(resolve(root, file)));
      }
      for (const id of [...this.getModuleIds()].sort()) {
        const info = this.getModuleInfo(id);
        if (info && !info.isExternal) add(relative(root, id), info.code ?? "");
      }
      for (const name of Object.keys(bundle).sort()) {
        const item = bundle[name]!;
        add(name, item.type === "chunk" ? item.code : item.source);
      }
      const digest = hash.digest("hex");
      for (const item of Object.values(bundle)) {
        if (item.type === "chunk")
          item.code = item.code.replaceAll(computeBuildPlaceholder, digest);
      }
    },
  };
}
