"use strict";

const fs = require("node:fs");
const path = require("node:path");
const os = require("node:os");
const { execFileSync } = require("node:child_process");

function bundle(directory) {
  const files = fs
    .readdirSync(directory)
    .filter((name) => name.endsWith(".cjs") && !name.endsWith(".test.cjs") && name !== "build.cjs");
  const factories = files
    .map((name) => {
      const source = fs.readFileSync(path.join(directory, name), "utf8").replace(/^#![^\n]*\n/, "");
      return `${JSON.stringify("./" + name)}:function(module,exports,require,__filename,__dirname){\n${source}\n}`;
    })
    .join(",\n");
  return `'use strict';\nconst builtinRequire=require;\nconst path=builtinRequire('node:path');\nconst factories={${factories}};\nconst cache={};\nfunction load(id){if(id.startsWith('node:'))return builtinRequire(id);if(!factories[id])throw new Error('Unbundled module '+id);if(cache[id])return cache[id].exports;const m={exports:{}};cache[id]=m;const r=load;r.main=cache['./cli.cjs'];factories[id](m,m.exports,r,path.join(path.dirname(process.execPath),id),path.dirname(process.execPath));return m.exports;}\nload('./cli.cjs');\n`;
}

function main(argv) {
  if (argv.length !== 1 || !path.isAbsolute(argv[0]))
    throw new Error("usage: node build.cjs /absolute/output-binary");
  if (Number(process.versions.node.split(".")[0]) < 26)
    throw new Error("Standalone packaging requires Node 26 or newer with --build-sea");
  const output = argv[0];
  if (fs.existsSync(output)) throw new Error("Output already exists; choose a new artifact path");
  fs.mkdirSync(path.dirname(output), { recursive: true });
  const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "server-swap-build-"));
  try {
    const entry = path.join(scratch, "bundle.cjs");
    const config = path.join(scratch, "sea.json");
    fs.writeFileSync(entry, bundle(__dirname));
    fs.writeFileSync(
      config,
      JSON.stringify({
        main: entry,
        output,
        disableExperimentalSEAWarning: true,
        useCodeCache: false,
      }),
    );
    execFileSync(process.execPath, [`--build-sea=${config}`], { stdio: "pipe", timeout: 120000 });
    if (process.platform === "darwin")
      execFileSync("/usr/bin/codesign", ["--force", "--sign", "-", output], {
        stdio: "pipe",
        timeout: 30000,
      });
    fs.chmodSync(output, 0o755);
  } finally {
    fs.rmSync(scratch, { recursive: true, force: true });
  }
  if (fs.existsSync(scratch)) throw new Error("Build scratch cleanup did not complete");
  process.stdout.write(
    JSON.stringify({
      status: "built",
      output,
      platform: process.platform,
      arch: process.arch,
      node: process.versions.node,
      scratchRemoved: true,
    }) + "\n",
  );
}

if (require.main === module) {
  try {
    main(process.argv.slice(2));
  } catch (error) {
    process.stderr.write(error.message + "\n");
    process.exitCode = 1;
  }
}
module.exports = { bundle, main };
