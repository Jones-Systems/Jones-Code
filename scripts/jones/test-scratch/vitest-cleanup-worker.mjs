import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

const [root, vitestPackage, mode] = process.argv.slice(2);
let context;
let cancelled = false;
const cancel = () => {
  cancelled = true;
  void context?.cancelCurrentRun("keyboard-input");
};
process.on("SIGINT", cancel);
process.on("SIGTERM", cancel);
process.on("message", cancel);
process.on("disconnect", cancel);

const fixture = NodePath.join(root, "fixture");
const scratch = NodePath.join(root, "tmp");
const observation = NodePath.join(root, "active.json");
const packageRoot = NodePath.dirname(vitestPackage);
const version = JSON.parse(NodeFS.readFileSync(vitestPackage, "utf8")).version;
let closed = false;
let primary;
try {
  NodeFS.mkdirSync(fixture);
  NodeFS.writeFileSync(NodePath.join(fixture, "lib.js"), "export const number = 1;\n");
  NodeFS.writeFileSync(
    NodePath.join(fixture, "tiny.test.js"),
    `import { test, expect } from ${JSON.stringify(NodeURL.pathToFileURL(NodePath.join(packageRoot, "dist/index.js")).href)};
import { number } from './lib.js';
import * as NodeFS from 'node:fs';
import * as NodePath from 'node:path';
test('temporary transformed modules: ${mode}', () => {
  const scratch = ${JSON.stringify(scratch)};
  const files = NodeFS.readdirSync(scratch, { recursive: true }).filter(name =>
    /^[0-9a-f]{40}$/.test(NodePath.basename(name)) &&
    /(?:^|[/\\\\])(?:ssr|client)(?:[/\\\\])/.test(name) &&
    NodeFS.statSync(NodePath.join(scratch, name)).isFile() &&
    NodeFS.statSync(NodePath.join(scratch, name)).size > 0);
  NodeFS.writeFileSync(${JSON.stringify(observation)}, JSON.stringify(files));
  expect(number).toBe(${mode === "pass" ? 1 : 2});
});\n`,
  );
  // Import only after the parent has assigned this child's temporary environment.
  const { createVitest } = await import(
    NodeURL.pathToFileURL(NodePath.join(packageRoot, "dist/node.js")).href
  );
  context = await createVitest(
    {
      root: fixture,
      config: false,
      watch: false,
      maxWorkers: 1,
      fileParallelism: false,
      pool: "forks",
      fsModuleCache: false,
      experimental: { viteModuleRunner: true },
      include: ["tiny.test.js"],
      reporters: ["dot"],
    },
    { envDir: false },
  );
  if (!cancelled) await context.start();
} catch (error) {
  primary = error;
} finally {
  try {
    if (context) {
      await context.close();
      closed = true;
    }
  } catch (closeError) {
    primary = primary
      ? new AggregateError([primary, closeError], "Vitest failed; close also failed")
      : closeError;
  } finally {
    NodeFS.writeFileSync(
      NodePath.join(root, "closed.json"),
      JSON.stringify({ version, closed, cancelled, remaining: NodeFS.readdirSync(scratch) }),
    );
    if (cancelled) process.exitCode = 130;
    if (process.connected) process.disconnect();
  }
}

if (primary) throw primary;
