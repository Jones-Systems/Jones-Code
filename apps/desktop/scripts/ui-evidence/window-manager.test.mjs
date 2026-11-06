import * as NodeTest from "node:test";
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeCrypto from "node:crypto";
import {
  prepareWindowManager,
  stageWindowManager,
  startWindowManager,
  readWindowManagerReady,
} from "./window-manager.mjs";
const hash = (bytes) => NodeCrypto.createHash("sha256").update(bytes).digest("hex");
async function fixture(t) {
  const base = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-private-wm-test-"));
  t.after(() =>
    NodeFSP.rm(base, {
      recursive: true,
      force: true,
    }),
  );
  const prefix = NodePath.join(base, "extract/root");
  for (const dir of ["usr/bin", "usr/lib", "usr/share/themes/Clearlooks/openbox-3"])
    await NodeFSP.mkdir(NodePath.join(prefix, dir), {
      recursive: true,
    });
  const executable = NodePath.join(prefix, "usr/bin/openbox");
  await NodeFSP.writeFile(executable, "test-only executable; never launched", {
    mode: 0o700,
  });
  await NodeFSP.writeFile(NodePath.join(prefix, "usr/lib/libfixture.so.1"), "test-only library");
  await NodeFSP.writeFile(
    NodePath.join(prefix, "usr/share/themes/Clearlooks/openbox-3/themerc"),
    "theme",
  );
  const archive = NodePath.join(base, "verified.deb");
  const archiveBytes = Buffer.from("synthetic provenance fixture; not an actual package");
  await NodeFSP.writeFile(archive, archiveBytes);
  const receipt = {
    schema: "jones-code.wm-prerequisite-result/v1",
    status: "prepared",
    host: {
      architecture: "amd64",
    },
    write_cone: base,
    packages: {
      items: [
        {
          local_file: archive,
          verified: true,
          sha256: hash(archiveBytes),
          size_bytes: archiveBytes.length,
        },
      ],
    },
    runtime: {
      version: "Openbox 3.6.1",
      executable,
      staged_libraries: ["libfixture.so.1"],
    },
  };
  const receiptPath = NodePath.join(base, "result.json");
  await NodeFSP.writeFile(receiptPath, JSON.stringify(receipt));
  return {
    base,
    prefix,
    executable,
    archive,
    receiptPath,
    receipt,
  };
}
NodeTest.test(
  "stage is a closed copy, with no default startup or menu command configuration",
  async (t) => {
    const f = await fixture(t);
    const prepared = await prepareWindowManager(f.receiptPath);
    const stage = NodePath.join(f.base, "stage");
    await NodeFSP.mkdir(stage);
    await stageWindowManager(prepared, stage);
    const root = NodePath.join(stage, "harness/support/window-manager");
    NodeAssert.equal(
      await NodeFSP.readFile(NodePath.join(root, "bin/openbox"), "utf8"),
      await NodeFSP.readFile(f.executable, "utf8"),
    );
    NodeAssert.equal(
      await NodeFSP.readFile(NodePath.join(root, "lib/libfixture.so.1"), "utf8"),
      "test-only library",
    );
    const config = await NodeFSP.readFile(NodePath.join(root, "rc.xml"), "utf8");
    NodeAssert.match(config, /<followMouse>no<\/followMouse>/);
    NodeAssert.doesNotMatch(config, /Execute|autostart|<menu|<keybind/);
    await NodeFSP.writeFile(f.executable, "changed original after staging");
    NodeAssert.equal(
      hash(await NodeFSP.readFile(NodePath.join(root, "bin/openbox"))),
      prepared.configuration.managedWindowManager.sha256,
    );
  },
);
NodeTest.test("archive tampering rejects preparation", async (t) => {
  const f = await fixture(t);
  await NodeFSP.appendFile(f.archive, "tampered");
  await NodeAssert.rejects(prepareWindowManager(f.receiptPath), /archive does not match/);
});
NodeTest.test(
  "changed runtime input rejects staging before the executable is copied",
  async (t) => {
    const f = await fixture(t);
    const prepared = await prepareWindowManager(f.receiptPath);
    await NodeFSP.appendFile(f.executable, "tampered");
    const stage = NodePath.join(f.base, "stage");
    await NodeFSP.mkdir(stage);
    await NodeAssert.rejects(stageWindowManager(prepared, stage), /input changed/);
    await NodeAssert.rejects(
      NodeFSP.stat(NodePath.join(stage, "harness/support/window-manager/bin/openbox")),
      {
        code: "ENOENT",
      },
    );
  },
);
NodeTest.test("an extracted executable symlink cannot escape the declared prefix", async (t) => {
  const f = await fixture(t);
  const outside = NodePath.join(f.base, "outside");
  await NodeFSP.writeFile(outside, "outside prefix");
  await NodeFSP.unlink(f.executable);
  await NodeFSP.symlink(outside, f.executable);
  await NodeAssert.rejects(prepareWindowManager(f.receiptPath), /escaped/);
});
NodeTest.test(
  "a traversal library name is rejected without reading outside the closure",
  async (t) => {
    const f = await fixture(t);
    f.receipt.runtime.staged_libraries = ["../../private.so.1"];
    await NodeFSP.writeFile(f.receiptPath, JSON.stringify(f.receipt));
    await NodeAssert.rejects(prepareWindowManager(f.receiptPath), /library name/);
  },
);
NodeTest.test("invalid runtime configuration never spawns a window manager", async () => {
  let registered = false;
  await NodeAssert.rejects(
    startWindowManager(
      {
        schema: "jones-private-window-manager/v1",
        executable: "/usr/bin/arbitrary",
      },
      ":0",
      () => {
        registered = true;
      },
    ),
    /Invalid private/,
  );
  NodeAssert.equal(registered, false);
});
NodeTest.test(
  "Openbox readiness uses its self-pointing EWMH window when PID is not published",
  () => {
    const root = "_NET_SUPPORTING_WM_CHECK(WINDOW): window id # 0x20000e\n";
    const peer = root + '_NET_WM_NAME(UTF8_STRING) = "Openbox"\n_NET_WM_PID: not found.\n';
    NodeAssert.deepEqual(readWindowManagerReady(root, peer, 16), {
      supportingWindow: "0x20000e",
      pidEvidence: "sole task-owned manager on fresh private display",
    });
  },
);
NodeTest.test("a foreign PID or inconsistent supporting window cannot qualify readiness", () => {
  const root = "_NET_SUPPORTING_WM_CHECK(WINDOW): window id # 0x20000e\n";
  const peer = root + '_NET_WM_NAME(UTF8_STRING) = "Openbox"\n_NET_WM_PID(CARDINAL) = 17\n';
  NodeAssert.equal(readWindowManagerReady(root, peer, 16), null);
  NodeAssert.equal(readWindowManagerReady(root, peer.replace("0x20000e", "0x40000e"), 17), null);
  NodeAssert.equal(readWindowManagerReady(root, peer.replace('"Openbox"', '"Other"'), 17), null);
});
