import * as NodeFS from "node:fs";

const argv = process.argv.slice(2);
const args = argv.join(" ");

function fail(message, code) {
  process.stderr.write(message + "\n");
  process.exit(code);
}

const permissionIndex = argv.indexOf("--permission-mode");
if (permissionIndex === -1 || argv[permissionIndex + 1] !== "dontAsk") {
  fail("text generation must deny permission prompts", 12);
}
const toolsIndex = argv.indexOf("--tools");
const investigation = process.env.T3_FAKE_CLAUDE_INVESTIGATION === "1";
if (toolsIndex === -1 || argv[toolsIndex + 1] !== (investigation ? "Read,Grep,Glob,Bash" : "")) {
  fail("text generation must receive the exact task tool set", 6);
}
if (investigation) {
  const allowed = argv[argv.indexOf("--allowedTools") + 1];
  const denied = argv[argv.indexOf("--disallowedTools") + 1];
  if (
    allowed !== "Read,Grep,Glob,Bash(git diff:*),Bash(git log:*),Bash(git show:*)" ||
    denied !== "Bash(git * --output*),Bash(git * -o *)"
  ) {
    fail("investigation must retain read-only tool restrictions", 17);
  }
}
if (argv.includes("--dangerously-skip-permissions")) {
  fail("text generation must not bypass permissions", 7);
}
if (!argv.includes("--disable-slash-commands")) {
  fail("text generation must disable skills", 8);
}
const managedMcpConfig = process.env.T3_FAKE_CLAUDE_MANAGED_MCP_CONFIG === "1";
if (
  managedMcpConfig ? argv.includes("--strict-mcp-config") : !argv.includes("--strict-mcp-config")
) {
  fail("text generation must respect managed MCP flag compatibility", 9);
}
if (process.env.T3_FAKE_CLAUDE_REJECTS_NO_SESSION_PERSISTENCE === "1") {
  if (argv.includes("--no-session-persistence")) {
    fail("error: unknown option '--no-session-persistence'", 1);
  }
} else if (!argv.includes("--no-session-persistence")) {
  fail("text generation must not save a resumable session", 16);
}
const settingsIndex = argv.indexOf("--settings");
if (settingsIndex === -1 || JSON.parse(argv[settingsIndex + 1]).disableAllHooks !== true) {
  fail("text generation must disable hooks", 10);
}
const cwdMustNotBe = process.env.T3_FAKE_CLAUDE_CWD_MUST_NOT_BE;
if (cwdMustNotBe && NodeFS.realpathSync(process.cwd()) === NodeFS.realpathSync(cwdMustNotBe)) {
  fail("text generation ran in the project directory", 11);
}

let stdinContent = "";
if (!process.stdin.isTTY) {
  const chunks = [];
  for await (const chunk of process.stdin) {
    chunks.push(chunk);
  }
  stdinContent = Buffer.concat(chunks).toString("utf8");
}

const argsMustContain = process.env.T3_FAKE_CLAUDE_ARGS_MUST_CONTAIN;
if (argsMustContain && !args.includes(argsMustContain)) {
  fail("args missing expected content", 2);
}

const argsMustNotContain = process.env.T3_FAKE_CLAUDE_ARGS_MUST_NOT_CONTAIN;
if (argsMustNotContain && args.includes(argsMustNotContain)) {
  fail("args contained forbidden content", 3);
}

const stdinMustContain = process.env.T3_FAKE_CLAUDE_STDIN_MUST_CONTAIN;
if (stdinMustContain && !stdinContent.includes(stdinMustContain)) {
  fail("stdin missing expected content", 4);
}

if (process.env.T3_FAKE_CLAUDE_EVALUATION === "1") {
  if (investigation || argv[argv.indexOf("--disallowedTools") + 1] !== "mcp__*") {
    fail("evaluation must deny every MCP tool and investigation", 18);
  }
  for (const flag of [
    "--allowedTools",
    "--chrome",
    "--mcp-config",
    "--plugin-dir",
    "--agents",
    "--add-dir",
  ]) {
    if (argv.includes(flag)) fail("evaluation must not enable task extensions", 19);
  }
}

const capturePath = process.env.T3_FAKE_CLAUDE_CAPTURE_PATH;
if (capturePath) {
  NodeFS.writeFileSync(
    capturePath,
    JSON.stringify({ argv, cwd: process.cwd(), stdin: stdinContent }),
  );
}

const configDirMustBe = process.env.T3_FAKE_CLAUDE_CONFIG_DIR_MUST_BE;
if (configDirMustBe && process.env.CLAUDE_CONFIG_DIR !== configDirMustBe) {
  fail("CLAUDE_CONFIG_DIR was " + (process.env.CLAUDE_CONFIG_DIR ?? ""), 5);
}

const stderrText = process.env.T3_FAKE_CLAUDE_STDERR;
if (stderrText) {
  process.stderr.write(stderrText + "\n");
}

process.stdout.write(process.env.T3_FAKE_CLAUDE_OUTPUT ?? "");
process.exitCode = Number(process.env.T3_FAKE_CLAUDE_EXIT_CODE ?? 0);
