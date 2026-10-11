import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { test } from "node:test";
import { cleanLaunchEnvironment, runOwnedChild } from "./native-startup-fixture.mjs";
import { protectedMutations } from "./native-startup-observer.mjs";

test("qualification removes pre-JS logging and Node-mode overrides while preserving the native home", () => {
  const env = cleanLaunchEnvironment(
    {
      HOME: "/runner",
      PATH: "/bin",
      ELECTRON_RUN_AS_NODE: "1",
      ELECTRON_ENABLE_LOGGING: "file",
      ELECTRON_LOG_FILE: "/unsafe",
      CHROME_LOG_FILE: "/unsafe",
      NODE_OPTIONS: "--require=unsafe",
      CFFIXED_USER_HOME: "/other",
      T3CODE_HOME: "/other",
      T3CODE_DESKTOP_USER_DATA_DIR: "/other",
      T3CODE_JONES_TRIAL_DESCRIPTOR: "/other",
      VITE_DEV_SERVER_URL: "http://other",
    },
    "/fixture",
  );
  assert.deepEqual(env, {
    HOME: "/runner",
    PATH: "/bin",
    TMPDIR: "/fixture",
    TMP: "/fixture",
    TEMP: "/fixture",
    NODE_DISABLE_COMPILE_CACHE: "1",
  });
});

test("observer preserves transient mutation evidence and fails closed on dropped or ambiguous history", () => {
  const events = [
    { path: "/profile/deleted-before-snapshot", flags: 0x100 | 0x200 },
    { path: "/profile-other/file", flags: 0x100 },
  ];
  assert.deepEqual(protectedMutations(events, ["/profile"]), [events[0]]);
  for (const flag of [0x01, 0x02, 0x04, 0x08, 0x20, 0x40, 0x80])
    assert.throws(
      () => protectedMutations([{ path: "/unrelated", flags: flag }], ["/profile"]),
      /history/,
    );
});

test("cancelled qualification reaps only its captured child before fixture removal", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "jones-qualifier-cancel-"));
  const marker = path.join(root, "pid");
  const controller = new AbortController();
  let completion;
  let timer;
  try {
    completion = runOwnedChild(
      process.execPath,
      [
        "-e",
        `require('node:fs').writeFileSync(${JSON.stringify(marker)}, String(process.pid)); setInterval(() => {}, 1000);`,
      ],
      { signal: controller.signal },
    );
    void completion.catch(() => {});
    // The exact child announces its PID; no process-name or host-wide scan is used.
    await new Promise((resolve, reject) => {
      const started = Date.now();
      timer = setInterval(async () => {
        try {
          await fs.stat(marker);
          clearInterval(timer);
          resolve();
        } catch (error) {
          if (error.code !== "ENOENT" || Date.now() - started > 5000) {
            clearInterval(timer);
            reject(error);
          }
        }
      }, 10);
    });
    const pid = Number(await fs.readFile(marker, "utf8"));
    controller.abort();
    await assert.rejects(completion, /cancelled/);
    assert.throws(() => process.kill(pid, 0), { code: "ESRCH" });
  } finally {
    clearInterval(timer);
    controller.abort();
    await completion?.catch(() => {});
    await fs.rm(root, { recursive: true, force: true });
  }
});
