import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import {
  assertCliPayloadUnchanged,
  cliComponentIdentifier,
  inventoryCliMachO,
  JONES_MACOS_CLI_ID,
  requireProductionCliBuild,
  signProductionCli,
} from "./cli.ts";
import { JONES_MACOS_APP_ID, JONES_MACOS_IDENTITY, JONES_MACOS_TEAM } from "./policy.ts";
import { productionTarget } from "./production-build.ts";

const configured = {
  JONES_MACOS_RELEASE_MODE: "production",
  JONES_MACOS_PRODUCTION_ID: JONES_MACOS_APP_ID,
  CSC_NAME: JONES_MACOS_IDENTITY,
  T3CODE_APPLE_TEAM_ID: JONES_MACOS_TEAM,
  CSC_KEYCHAIN: "/synthetic/job.keychain-db",
  JONES_NOTARY_KEY_FILE: "/synthetic/notary.p8",
  APPLE_NOTARY_KEY_ID: "synthetic-public-key-id",
  APPLE_NOTARY_ISSUER_ID: "synthetic-public-issuer",
};

function fixture() {
  const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "jones-cli-signing-test-"));
  const contentDir = NodePath.join(root, "t3-synthetic-darwin-arm64");
  const entitlements = NodePath.join(root, "cli-entitlements.plist");
  const sourceEntitlements = NodeFS.readFileSync(
    new URL("../../../apps/server/resources/cli-entitlements.plist", import.meta.url),
  );
  NodeFS.writeFileSync(entitlements, sourceEntitlements);
  const names = [
    "t3",
    "node_modules/native/addon.node",
    "node_modules/other/unlisted-helper",
    "resource-monitor/darwin-arm64/t3-resource-monitor",
  ];
  function put(name, bytes) {
    const path = NodePath.join(contentDir, name);
    NodeFS.mkdirSync(NodePath.dirname(path), { recursive: true });
    NodeFS.writeFileSync(path, bytes, { mode: 0o755 });
  }
  for (const name of names) put(name, Buffer.from("cffaedfe00000000", "hex"));
  put("client/index.html", "synthetic web client");
  put("node_modules/native/not-native.node", "ordinary text with a native extension");
  const calls = [];
  const identifiers = new Map();
  let notaryScratch;
  let submitted;
  function runner(fault) {
    return (command, args) => {
      calls.push({ command, args });
      if (command === "/usr/bin/codesign") {
        const component = args.at(-1);
        if (args.includes("--sign")) {
          identifiers.set(component, args[args.indexOf("--identifier") + 1]);
          if (fault === "sign-command") throw new Error("synthetic signing failure");
        } else if (args.includes("--verify")) {
          NodeAssert.ok(args[args.indexOf("-R") + 1].startsWith("=identifier "));
          NodeAssert.ok(args[args.indexOf("-R") + 1].includes(JONES_MACOS_TEAM));
          if (fault === "verify-command") throw new Error("synthetic signature mismatch");
        } else {
          const identifier = fault === "wrong-id" ? "wrong.identifier" : identifiers.get(component);
          const team = fault === "wrong-team" ? "WRONGTEAM1" : JONES_MACOS_TEAM;
          const authority =
            fault === "adhoc" ? "Signature=adhoc" : `Authority=${JONES_MACOS_IDENTITY}`;
          return {
            stdout: "",
            stderr: `Identifier=${identifier}\nTeamIdentifier=${team}\n${authority}\n${fault === "no-timestamp" ? "" : "Timestamp=synthetic"}\n${fault === "no-runtime" ? "" : "CodeDirectory flags=0x10000(runtime)"}`,
          };
        }
      } else if (command === "/usr/bin/ditto") {
        NodeAssert.equal(args.at(-2), contentDir, "notarize the whole staged distribution");
        NodeAssert.ok(args.includes("--keepParent"));
        notaryScratch = NodePath.dirname(args.at(-1));
        NodeAssert.notEqual(notaryScratch, contentDir);
        submitted = Object.fromEntries(
          [...names, "client/index.html", "node_modules/native/not-native.node"].map((name) => [
            name,
            NodeFS.readFileSync(NodePath.join(contentDir, name)).toString("hex"),
          ]),
        );
        NodeFS.writeFileSync(args.at(-1), JSON.stringify(submitted));
        if (fault === "zip-command") throw new Error("synthetic zip failure");
      } else if (command === "/usr/bin/xcrun") {
        NodeAssert.equal(args[0], "notarytool");
        NodeAssert.deepEqual(JSON.parse(NodeFS.readFileSync(args[2], "utf8")), submitted);
        NodeAssert.equal(
          identifiers.size,
          names.length,
          "all shipped Mach-O signed before submission",
        );
        if (fault === "cancel-command") throw new Error("synthetic interrupted command");
        if (fault === "payload-mutation") put("t3", "changed after ZIP");
        return {
          stdout:
            fault === "malformed-notary"
              ? "invalid-json"
              : JSON.stringify({
                  status:
                    fault === "notary-rejected"
                      ? "Invalid"
                      : fault === "notary-pending"
                        ? "In Progress"
                        : "Accepted",
                }),
          stderr: "",
        };
      } else NodeAssert.fail(`unexpected command ${command}`);
      return { stdout: "", stderr: "" };
    };
  }
  return {
    root,
    contentDir,
    entitlements,
    names,
    put,
    calls,
    identifiers,
    runner,
    get notaryScratch() {
      return notaryScratch;
    },
    get submitted() {
      return submitted;
    },
    cleanup() {
      NodeFS.rmSync(root, { recursive: true, force: true });
    },
  };
}

NodeTest.test(
  "CLI identities are stable, separate from desktop, and distinct per component",
  () => {
    NodeAssert.equal(cliComponentIdentifier("/one/stage", "/one/stage/t3"), JONES_MACOS_CLI_ID);
    NodeAssert.notEqual(JONES_MACOS_CLI_ID, JONES_MACOS_APP_ID);
    const first = cliComponentIdentifier(
      "/one",
      "/one/node_modules/.pnpm/native@1/node_modules/native/addon.node",
    );
    NodeAssert.equal(
      first,
      cliComponentIdentifier(
        "/two",
        "/two/node_modules/.pnpm/native@2/node_modules/native/addon.node",
      ),
    );
    NodeAssert.ok(first.startsWith(`${JONES_MACOS_CLI_ID}.component.`));
    NodeAssert.notEqual(first, cliComponentIdentifier("/one", "/one/other/addon.node"));
    NodeAssert.throws(() => cliComponentIdentifier("/one", "/outside/t3"));
    NodeAssert.equal(productionTarget(), "desktop");
    NodeAssert.equal(productionTarget("cli"), "cli");
    NodeAssert.throws(() => productionTarget("arbitrary-command"));
  },
);

NodeTest.test("production CLI configuration fails closed before any signing command", () => {
  NodeAssert.equal(requireProductionCliBuild({}, { platform: "mac", arch: "arm64" }), false);
  NodeAssert.equal(requireProductionCliBuild(configured, { platform: "mac", arch: "arm64" }), true);
  for (const key of Object.keys(configured)) {
    if (key === "JONES_MACOS_RELEASE_MODE") continue;
    NodeAssert.throws(
      () =>
        requireProductionCliBuild({ ...configured, [key]: "" }, { platform: "mac", arch: "arm64" }),
      key,
    );
  }
  for (const patch of [{ CSC_NAME: "-" }, { T3CODE_APPLE_TEAM_ID: "WRONGTEAM1" }]) {
    NodeAssert.throws(() =>
      requireProductionCliBuild({ ...configured, ...patch }, { platform: "mac", arch: "arm64" }),
    );
  }
  for (const target of [
    { platform: "linux", arch: "arm64" },
    { platform: "mac", arch: "x64" },
  ]) {
    NodeAssert.throws(() => requireProductionCliBuild(configured, target));
  }
});

NodeTest.test(
  "complete Mach-O inventory, preserved entitlements, notary acceptance, and tar bytes agree",
  () => {
    const f = fixture();
    try {
      const originalEntitlements = NodeFS.readFileSync(f.entitlements);
      const inventory = inventoryCliMachO(f.contentDir);
      NodeAssert.equal(inventory.length, f.names.length);
      NodeAssert.equal(NodePath.basename(inventory.at(-1)), "t3");
      const seal = signProductionCli(f, configured, f.runner());
      assertCliPayloadUnchanged(f.contentDir, seal);
      NodeAssert.deepEqual(NodeFS.readFileSync(f.entitlements), originalEntitlements);
      const signatures = f.calls.filter(({ args }) => args.includes("--sign"));
      NodeAssert.equal(signatures.length, f.names.length);
      NodeAssert.equal(new Set(f.identifiers.values()).size, f.names.length);
      for (const { args } of signatures) {
        NodeAssert.equal(args[args.indexOf("--sign") + 1], JONES_MACOS_IDENTITY);
        NodeAssert.ok(args.includes("--timestamp") && args.includes("runtime"));
        if (NodePath.basename(args.at(-1)) === "t3")
          NodeAssert.equal(args[args.indexOf("--entitlements") + 1], f.entitlements);
        else NodeAssert.ok(args.includes("--preserve-metadata=entitlements"));
      }
      NodeAssert.equal(NodePath.basename(signatures.at(-1).args.at(-1)), "t3");
      NodeAssert.equal(
        f.calls.some(({ args }) => args.includes("stapler")),
        false,
      );
      NodeAssert.ok(f.notaryScratch && !NodeFS.existsSync(f.notaryScratch));
      const archive = NodePath.join(f.root, "distribution.tar.gz");
      NodeChildProcess.execFileSync("tar", [
        "-czf",
        archive,
        "-C",
        f.root,
        NodePath.basename(f.contentDir),
      ]);
      const extracted = NodePath.join(f.root, "unpacked");
      NodeFS.mkdirSync(extracted);
      NodeChildProcess.execFileSync("tar", ["-xzf", archive, "-C", extracted]);
      const shipped = NodePath.join(extracted, NodePath.basename(f.contentDir));
      assertCliPayloadUnchanged(shipped, seal);
      for (const [name, bytes] of Object.entries(f.submitted))
        NodeAssert.equal(NodeFS.readFileSync(NodePath.join(shipped, name)).toString("hex"), bytes);
      f.put("t3", "modified after notarization");
      NodeAssert.throws(() => assertCliPayloadUnchanged(f.contentDir, seal), /changed/);
    } finally {
      f.cleanup();
    }
  },
);

for (const fault of [
  "sign-command",
  "verify-command",
  "wrong-id",
  "wrong-team",
  "adhoc",
  "no-timestamp",
  "no-runtime",
  "zip-command",
  "notary-rejected",
  "notary-pending",
  "malformed-notary",
  "payload-mutation",
  "cancel-command",
]) {
  NodeTest.test(`CLI signing refuses ${fault} and removes owned notary scratch`, () => {
    const f = fixture();
    try {
      NodeAssert.throws(() => signProductionCli(f, configured, f.runner(fault)));
      if (f.notaryScratch) NodeAssert.equal(NodeFS.existsSync(f.notaryScratch), false);
      if (
        ![
          "zip-command",
          "notary-rejected",
          "notary-pending",
          "malformed-notary",
          "payload-mutation",
          "cancel-command",
        ].includes(fault)
      ) {
        NodeAssert.equal(
          f.calls.some(({ command }) => command === "/usr/bin/xcrun"),
          false,
        );
      }
    } finally {
      f.cleanup();
    }
  });
}

NodeTest.test("OpenAI-owned native binaries and symlink payloads stop before signing", () => {
  for (const fault of ["openai", "symlink", "non-native-main", "duplicate-id"]) {
    const f = fixture();
    try {
      if (fault === "openai")
        f.put("node_modules/@openai/codex/codex", Buffer.from("cffaedfe00000000", "hex"));
      if (fault === "symlink")
        NodeFS.symlinkSync(f.entitlements, NodePath.join(f.contentDir, "outside"));
      if (fault === "non-native-main") f.put("t3", "not native");
      if (fault === "duplicate-id") {
        for (const version of ["1", "2"])
          f.put(
            `node_modules/.pnpm/native@${version}/node_modules/native/addon.node`,
            Buffer.from("cffaedfe00000000", "hex"),
          );
      }
      NodeAssert.throws(() => signProductionCli(f, configured, f.runner()));
      NodeAssert.equal(f.calls.length, 0);
    } finally {
      f.cleanup();
    }
  }
});
