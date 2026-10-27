import { describe, expect, it } from "vite-plus/test";
import { isExpectedCodexBackgroundTermination } from "./CodexBackgroundCompletion.ts";

const server = "timeout 600 python3 -m http.server 18763 --bind 127.0.0.1 --directory .t3/pr-media";

describe("expected Codex background termination", () => {
  it.each([
    server,
    `/bin/bash -lc '${server}'`,
    `/bin/zsh -lc "${server}"`,
    "timeout -k 5s --foreground 10m python3 -m http.server 8000",
    "timeout --signal TERM 1.5m /usr/bin/python3.13 -u -B -m http.server",
    'timeout -- 600 python -m http.server --directory "media files"',
  ])("recognizes a bounded static server expiry: %s", (command) => {
    expect(isExpectedCodexBackgroundTermination({ command, exitCode: 124 })).toBe(true);
  });

  it.each([0, 1, 2, 125, 126, 127, 137, 143, null, undefined])(
    "retains other server outcomes (%s)",
    (exitCode) => {
      expect(isExpectedCodexBackgroundTermination({ command: server, exitCode })).toBe(false);
    },
  );

  it.each([
    "timeout 600 pytest",
    "timeout 600 node server.js",
    "python3 -m http.server",
    "timeout --preserve-status 600 python3 -m http.server",
    "timeout --signal 600 python3 -m http.server",
    "timeout -k nope 600 python3 -m http.server",
    "timeout 0 python3 -m http.server",
    "timeout nope python3 -m http.server",
    "timeout 600 python3 -c 'import http.server'",
    "timeout 600 python3 -m http.server_extra",
    `${server} && echo done`,
    `${server}; echo done`,
    `${server} | cat`,
    `${server} > log 2>&1`,
    `${server}\necho done`,
    `${server} --directory $MEDIA`,
    `${server} --directory $(pwd)`,
    `${server} --directory \`pwd\``,
    `${server} --directory 'unterminated`,
    `/bin/bash -lc '${server}' && echo done`,
    `/bin/bash -lc '/bin/bash -lc "${server}"'`,
    "echo 'timeout 600 python3 -m http.server'",
  ])("keeps unrelated, ambiguous or compound commands visible: %s", (command) => {
    expect(isExpectedCodexBackgroundTermination({ command, exitCode: 124 })).toBe(false);
  });
});
