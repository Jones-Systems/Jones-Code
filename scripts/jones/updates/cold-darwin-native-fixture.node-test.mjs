import * as NodeTest from "node:test";
import * as NodeAssert from "node:assert/strict";
import {
  coldFixtureFailure,
  coldFixtureEnvironment,
  runColdDarwinNativeFixture,
} from "./cold-darwin-native-fixture.mjs";

NodeTest.test("native fixture import is inert and diagnostics never retain raw stderr", () => {
  NodeAssert.equal(typeof runColdDarwinNativeFixture, "function");
  NodeAssert.deepEqual(coldFixtureFailure("private diagnostic 0xfeedface"), {
    stderrPresent: true,
    stderrTruncated: false,
    stderrClass: "redacted",
  });
  NodeAssert.equal(coldFixtureFailure("x".repeat(4097)).stderrTruncated, true);
  NodeAssert.equal(coldFixtureFailure("COLD_FIXTURE_FAILURE").stderrClass, "fixture-rejected");
});

NodeTest.test("fixture rejects an unowned root before creating resources", async () => {
  for (const root of ["relative", "/tmp", "/tmp/other-fixture"]) {
    await NodeAssert.rejects(runColdDarwinNativeFixture({ root }), /Invalid owned fixture root/);
  }
});

NodeTest.test(
  "child environment is a fixed private configuration without ambient injection",
  () => {
    // oxlint-disable-next-line t3code/no-global-process-runtime -- This standalone fixture checks the actual host before creating resources, without loading the application runtime.
    if (process.platform !== "darwin" || typeof process.getuid !== "function") {
      NodeAssert.throws(() => coldFixtureEnvironment("/owned-fixture"), /Darwin real UID/);
      return;
    }
    const environment = coldFixtureEnvironment("/owned-fixture");
    NodeAssert.deepEqual(environment, {
      PATH: "/usr/bin:/bin",
      LANG: "C",
      LC_ALL: "C",
      HOME: "/owned-fixture/home",
      __CF_USER_TEXT_ENCODING: `0x${process.getuid().toString(16).toUpperCase()}:0:0`,
      CFFIXED_USER_HOME: "/owned-fixture/home",
      XDG_CONFIG_HOME: "/owned-fixture/config",
      XDG_CACHE_HOME: "/owned-fixture/cache",
      TMPDIR: "/owned-fixture",
      CLANG_MODULE_CACHE_PATH: "/owned-fixture/cache",
      ELECTRON_RUN_AS_NODE: "1",
      BREAKPAD_DUMP_LOCATION: "/owned-fixture",
    });
    for (const name of [
      "NODE_OPTIONS",
      "NODE_PATH",
      "PYTHONPATH",
      "PYTHONHOME",
      "DYLD_INSERT_LIBRARIES",
      "CODEX_HOME",
      "SSH_AUTH_SOCK",
    ])
      NodeAssert.equal(Object.hasOwn(environment, name), false);
    NodeAssert.equal(Object.isFrozen(environment), true);
  },
);
