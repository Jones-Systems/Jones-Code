// @effect-diagnostics nodeBuiltinImport:off - CLI fixtures drive the real transport with bounded synthetic subprocess responses.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { afterEach, expect, it, vi } from "vite-plus/test";

const fixture = vi.hoisted(() => ({
  mode: "compare" as "compare" | "truncated" | "404" | "failed-200" | "invalid-json",
  calls: [] as string[][],
  observed: undefined as unknown,
}));
const base = "a".repeat(40),
  head = "b".repeat(40);
const endpoint = `repos/Jones-Systems/Jones-Code/compare/${base}...${head}`;
vi.mock("node:child_process", async () => {
  const { promisify } = await import("node:util");
  const execFile = () => {};
  Object.defineProperty(execFile, promisify.custom, {
    value: async (_command: string, args: string[]) => {
      fixture.calls.push(args);
      if (fixture.mode === "404")
        throw Object.assign(new Error("HTTP failure"), {
          code: 1,
          stdout:
            'HTTP/2.0 404 Not Found\nContent-Type: application/json\n\n{"message":"Not Found"}\n',
        });
      if (fixture.mode === "failed-200")
        throw Object.assign(new Error("process failed"), {
          code: 1,
          stdout: 'HTTP/2.0 200 OK\nContent-Type: application/json\n\n{"status":"ahead"}\n',
        });
      if (fixture.mode === "invalid-json")
        return {
          stdout: 'HTTP/2.0 200 OK\nContent-Type: application/json\n\n{"RAW_RESPONSE_MARKER":',
          stderr: "",
        };
      if (fixture.mode === "truncated" || !args.includes("--jq"))
        throw Object.assign(new Error("stdout maxBuffer length exceeded"), {
          code: "ERR_CHILD_PROCESS_STDIO_MAXBUFFER",
          stdout:
            'HTTP/2.0 200 OK\nContent-Type: application/json\n\n{"RAW_RESPONSE_MARKER":"' +
            "x".repeat(2 * 1024 * 1024),
        });
      return {
        stdout:
          "HTTP/2.0 200 OK\nContent-Type: application/json\n\n" +
          JSON.stringify({ status: "ahead", merge_base_commit: { sha: "a".repeat(40) } }),
        stderr: "",
      };
    },
  });
  return { execFile };
});
vi.mock("../../../apps/server/src/jones/updates/recoverQualifiedStage.ts", () => ({
  inspectQualifiedStageRecovery: async (
    _input: unknown,
    api: { get: (endpoint: string) => Promise<unknown> },
  ) => {
    fixture.observed = await api.get(
      fixture.mode === "404" ? "repos/Jones-Systems/Jones-Code/actions/artifacts/103" : endpoint,
    );
    return fixture.observed;
  },
  applyQualifiedStageRecovery: () => {
    throw new Error("Apply must not run in transport inspection fixtures.");
  },
}));

const roots: string[] = [];
const originalArguments = process.argv;
afterEach(async () => {
  process.argv = originalArguments;
  vi.restoreAllMocks();
  fixture.calls.length = 0;
  fixture.observed = undefined;
  for (const root of roots.splice(0)) {
    await NodeFSP.rm(root, { recursive: true, force: true });
    await expect(NodeFSP.lstat(root)).rejects.toMatchObject({ code: "ENOENT" });
  }
});

async function run(mode: typeof fixture.mode) {
  fixture.mode = mode;
  const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-recovery-cli-"));
  roots.push(root);
  const input = NodePath.join(root, "input.json");
  await NodeFSP.writeFile(input, "{}");
  process.argv = [process.execPath, "recover-qualified-stage.ts", input];
  vi.spyOn(process.stdout, "write").mockImplementation(() => true);
  vi.resetModules();
  return import("./recover-qualified-stage.ts");
}

it("projects ancestry in gh before buffering while preserving HTTP status and exact merge base", async () => {
  await run("compare");
  expect(fixture.calls[0]).toContain("--include");
  expect(fixture.calls[0]).toContain("--jq");
  expect(fixture.calls[0]).toContain("{status, merge_base_commit: {sha: .merge_base_commit.sha}}");
  expect(fixture.observed).toEqual({
    status: 200,
    body: { status: "ahead", merge_base_commit: { sha: base } },
  });
});

it.each(["truncated", "failed-200", "invalid-json"] as const)(
  "refuses %s with bounded diagnostics and no response-body disclosure",
  async (mode) => {
    const failure = await run(mode).then(
      () => undefined,
      (cause: unknown) => cause,
    );
    expect(failure).toBeInstanceOf(Error);
    const error = failure as Error;
    expect(error.message.length).toBeLessThan(160);
    expect(error.message).not.toContain("RAW_RESPONSE_MARKER");
    expect(error.cause).toBeUndefined();
    expect(fixture.observed).toBeUndefined();
  },
);

it("preserves a complete HTTP404 response as unavailable metadata without ancestry projection", async () => {
  await run("404");
  expect(fixture.observed).toEqual({ status: 404, body: { message: "Not Found" } });
  expect(fixture.calls[0]).not.toContain("--jq");
});
