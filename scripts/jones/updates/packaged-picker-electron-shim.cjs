"use strict";
const fs = require("node:fs");
const path = require("node:path");
const argument = process.argv.filter((value) => value.startsWith("--jones-picker-config="));
if (argument.length !== 1) throw new Error("An exact owned picker configuration is required.");
const file = argument[0].slice("--jones-picker-config=".length);
const config = JSON.parse(fs.readFileSync(file, "utf8"));
if (
  fs.realpathSync(config.root) !== config.root ||
  !file.startsWith(config.root + path.sep) ||
  fs.readFileSync(path.join(config.root, ".picker-owner"), "utf8") !== config.nonce
)
  throw new Error("Picker loader ownership failed.");
if (
  fs.existsSync(path.join(process.resourcesPath, "app.asar")) ||
  config.appRoot !== path.join(process.resourcesPath, "picker-retained.asar")
)
  throw new Error("Unsafe picker COPY layout.");
fs.writeFileSync(
  path.join(config.root, `loader-${config.phase}.json`),
  JSON.stringify({
    layout: "modified disposable COPY",
    archive: config.appRoot,
    main: config.main,
  }),
);
require(config.driver).run(config);
