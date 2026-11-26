import type { Context, ESTree } from "@oxlint/plugins";
import * as NodeURL from "node:url";

import baseline from "./jones-untranslated-copy-baseline.json" with { type: "json" };

const repositoryRoot = new URL("../../", import.meta.url).href;
type Occurrences = Readonly<Record<string, number | undefined>>;
const files: Readonly<Record<string, Occurrences | undefined>> = baseline.files;

export function matchJonesLegacyCopy(context: Pick<Context, "filename" | "sourceCode">) {
  const file = NodeURL.pathToFileURL(context.filename);
  if (!file.href.startsWith(repositoryRoot) || NodeURL.fileURLToPath(file) !== context.filename) {
    return undefined;
  }
  const path = decodeURIComponent(file.href.slice(repositoryRoot.length));
  const allowed = Object.hasOwn(files, path) ? files[path] : undefined;
  if (allowed === undefined) return undefined;

  // Oxlint calls create once per file. Keep the occurrence budget local to that visit.
  const matched = new Map<string, number>();
  return (node: ESTree.Node): boolean => {
    const key = `${node.type}/${node.parent?.type}:${context.sourceCode.getText(node)}`;
    const count = matched.get(key) ?? 0;
    if (count >= (allowed[key] ?? 0)) return false;
    matched.set(key, count + 1);
    return true;
  };
}
