// @effect-diagnostics nodeBuiltinImport:off -- Hash synthetic original bytes in memory; no keys or plaintext files are created.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as Fiber from "effect/Fiber";
import * as CliSessions from "../runtime/peerSetupCli.ts";
import { expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import * as Sink from "effect/Sink";
import * as Stdio from "effect/Stdio";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import { Command } from "effect/cli";
import { workstreamCommand } from "../../../../cli/workstream.ts";
import { runNativeReadExportCli, makeNativeExportCliCommand } from "./exportCli.ts";

const at = "2026-10-25T00:00:00Z";
const envelope = {
  contract: "workstreams-read/1.0.0",
  registry: {
    registry_instance_id: "8cdd84b6-81c5-4c88-9d13-5d324abaf662",
    owner_id: "owner",
    canonical_origin: "https://registry.test",
    server_generation: 7,
    registry_version: 11,
  },
  provenance: {
    authority: "native-home",
    home_environment_id: "env:a",
    served_by_environment_id: "env:a",
    source: "live",
    observed_at: at,
    stale: false,
  },
  coverage: { registry: "complete", sources: [] },
};
const counts = {
  context: { owner_id: "owner", server_generation: 7, registry_version: 11 },
  principal_id: "principal",
  observed_at: at,
  authority_effect: "none",
  total: 1,
  active: 1,
  unknown_lifecycle: 0,
};
const countsResult = () => ({
  contract: "workstreams-native-home-read-result/1.0.0",
  route: "counts",
  authorization_revision: 1,
  envelope: structuredClone(envelope),
  value: structuredClone(counts),
});
const item = {
  workstream: {
    workstream_id: "a",
    owner_id: "owner",
    name: '雪 😀 "quote"\nnext',
    lifecycle: "active",
    progress: { state: "unknown" },
    delivery: "none-observed",
    freshness: "unknown",
    sort_order: 0,
    version: 1,
    created_at: at,
    updated_at: at,
    created_by: { principal_id: "principal" },
    updated_by: { principal_id: "principal" },
  },
  active_memberships: { primary: 0, secondary: 0 },
  appearance: { workstream_id: "a", border_color: null, version: 0 },
  lifecycle: null,
};
const page =
  "  " +
  JSON.stringify({ envelope, counts, offset: 0, items: [item] }).replace("quote", "\\u0071uote") +
  " \r\n";
const raw = new TextEncoder().encode(
  page +
    JSON.stringify({
      complete: true,
      registry_version: 11,
      sha256: NodeCrypto.createHash("sha256").update(page).digest("hex"),
    }) +
    "\n",
);
const fixture = (
  options: {
    raw?: Uint8Array;
    status?: number;
    type?: string;
    after?: (value: ReturnType<typeof countsResult>) => void;
    breakStream?: boolean;
    chunkSize?: number;
    hold?: { ready: () => void; cancelled: () => void };
  } = {},
) => {
  const output: Uint8Array[] = [],
    calls: string[] = [];
  let observations = 0;
  const http = HttpClient.make((request) =>
    Effect.sync(() => {
      calls.push(request.url);
      expect(request.headers.authorization).toBe("Bearer synthetic-private-token");
      if (request.url.endsWith("/read")) {
        expect(request.method).toBe("POST");
        const value = countsResult();
        if (++observations === 2) options.after?.(value);
        return HttpClientResponse.fromWeb(request, Response.json(value));
      }
      expect(request.method).toBe("GET");
      const source = options.raw ?? raw;
      const body = new ReadableStream<Uint8Array>(
        {
          pull() {
            if (options.hold) {
              options.hold.ready();
              return new Promise<void>(() => {});
            }
          },
          cancel() {
            options.hold?.cancelled();
          },
          start(controller) {
            if (options.hold) return;
            if (options.breakStream) {
              controller.enqueue(source.slice(0, 7));
              controller.error(new Error("synthetic transport closed"));
              return;
            }
            for (let i = 0; i < source.length; i += options.chunkSize ?? 7)
              controller.enqueue(source.slice(i, i + (options.chunkSize ?? 7)));
            controller.close();
          },
        },
        { highWaterMark: 0 },
      );
      return HttpClientResponse.fromWeb(
        request,
        new Response(body, {
          status: options.status ?? 200,
          headers: { "content-type": options.type ?? "application/x-ndjson; charset=utf-8" },
        }),
      );
    }),
  );
  const stdio = Stdio.make({
    args: Effect.succeed([]),
    stdin: Stream.empty,
    stdout: () =>
      Sink.forEach((part: string | Uint8Array) =>
        Effect.sync(() =>
          output.push(typeof part === "string" ? new TextEncoder().encode(part) : part.slice()),
        ),
      ),
    stderr: () => Sink.drain,
  });
  const run = (origin = "http://127.0.0.1:1111", pageSize = 1) =>
    runNativeReadExportCli(
      origin,
      { authorization: "Bearer synthetic-private-token" },
      pageSize,
    ).pipe(
      Effect.provideService(HttpClient.HttpClient, http),
      Effect.provideService(Stdio.Stdio, stdio),
      Effect.scoped,
    );
  return { output, calls, run };
};
it("registers export in the real CLI assembly", () => {
  expect(
    workstreamCommand.subcommands.flatMap((group) => group.commands.map((command) => command.name)),
  ).toContain("export");
});
it.effect(
  "parses selected home and bounded page size without accepting URLs, callers or credentials",
  () =>
    Effect.gen(function* () {
      const seen: unknown[] = [];
      const run = Command.runWith(
        makeNativeExportCliCommand((home, pageSize) =>
          Effect.sync(() => {
            seen.push({ home, pageSize });
          }),
        ),
        { version: "test" },
      );
      yield* run(["--home", "/synthetic/home"]);
      yield* run(["--home", "/synthetic/home", "--page-size", "1"]);
      expect(seen).toEqual([
        { home: "/synthetic/home", pageSize: 100 },
        { home: "/synthetic/home", pageSize: 1 },
      ]);
      for (const args of [
        ["--home", "/synthetic/home", "--page-size", "0"],
        ["--home", "/synthetic/home", "--page-size", "101"],
        ["--home", "/synthetic/home", "--url", "http://forged"],
        ["--home", "/synthetic/home", "--token", "forged"],
      ])
        expect((yield* run(args).pipe(Effect.result))._tag).toBe("Failure");
      expect(seen).toHaveLength(2);
    }).pipe(Effect.provide(NodeServices.layer)),
);
it.effect(
  "writes only original validated bytes after EOF and a fresh matching read observation",
  () =>
    Effect.gen(function* () {
      const f = fixture();
      yield* f.run();
      expect(Buffer.concat(f.output)).toEqual(Buffer.from(raw));
      expect(f.calls).toEqual([
        "http://127.0.0.1:1111/api/workstreams/native/v1/read",
        "http://127.0.0.1:1111/api/workstreams/native/v1/export?page_size=1",
        "http://127.0.0.1:1111/api/workstreams/native/v1/read",
      ]);
    }),
);
it.effect.each([401, 403, 404, 405, 409, 503])(
  "refuses export HTTP %s without fallback or stdout",
  (status) =>
    Effect.gen(function* () {
      const f = fixture({ status });
      expect((yield* f.run().pipe(Effect.result))._tag).toBe("Failure");
      expect(f.output).toEqual([]);
      expect(f.calls).toHaveLength(2);
    }),
);
it.effect.each([
  "truncated",
  "bad-digest",
  "trailing",
  "invalid-utf8",
  "wrong-type",
  "stream-failure",
] as const)("publishes no bytes for %s exports", (fault) =>
  Effect.gen(function* () {
    const source =
      fault === "truncated"
        ? raw.slice(0, raw.length - 10)
        : fault === "bad-digest"
          ? new TextEncoder().encode(
              new TextDecoder()
                .decode(raw)
                .replace(/"sha256":"[a-f0-9]{64}"/, '"sha256":"' + "0".repeat(64) + '"'),
            )
          : fault === "trailing"
            ? new Uint8Array([...raw, 10])
            : fault === "invalid-utf8"
              ? new Uint8Array([255, ...raw])
              : raw;
    const f = fixture({
      raw: source,
      ...(fault === "wrong-type" ? { type: "application/json" } : {}),
      breakStream: fault === "stream-failure",
    });
    expect((yield* f.run().pipe(Effect.result))._tag).toBe("Failure");
    expect(f.output).toEqual([]);
  }),
);
it.effect.each([
  "authorization",
  "principal",
  "generation",
  "registry",
  "home",
  "stale",
  "coverage",
  "counts-context",
  "counts-observation",
] as const)("refuses changed final %s before stdout", (field) =>
  Effect.gen(function* () {
    const f = fixture({
      after: (value) => {
        if (field === "authorization") value.authorization_revision++;
        if (field === "principal") value.value.principal_id = "replacement";
        if (field === "generation") value.envelope.registry.server_generation++;
        if (field === "registry") value.envelope.registry.registry_version++;
        if (field === "home") value.envelope.provenance.home_environment_id = "replacement";
        if (field === "stale") value.envelope.provenance.stale = true;
        if (field === "coverage") value.envelope.coverage.registry = "unavailable";
        if (field === "counts-context") value.value.context.owner_id = "replacement";
        if (field === "counts-observation") value.value.observed_at = "2026-10-26T00:00:00Z";
      },
    });
    expect((yield* f.run().pipe(Effect.result))._tag).toBe("Failure");
    expect(f.output).toEqual([]);
    expect(f.calls).toHaveLength(3);
  }),
);
it.effect("refuses unselected origins and bounds before any request", () =>
  Effect.gen(function* () {
    for (const origin of [
      "https://remote.test",
      "http://user:secret@127.0.0.1:1111",
      "http://127.0.0.1:1111/path",
      "http://127.0.0.1",
    ]) {
      const f = fixture();
      expect((yield* f.run(origin).pipe(Effect.result))._tag).toBe("Failure");
      expect(f.calls).toEqual([]);
    }
    for (const size of [0, 101, 1.5, NaN]) {
      const f = fixture();
      expect((yield* f.run(undefined, size).pipe(Effect.result))._tag).toBe("Failure");
      expect(f.calls).toEqual([]);
    }
  }),
);

it.effect("rejects the byte bound before publishing or performing a final observation", () =>
  Effect.gen(function* () {
    const oversized = new Uint8Array(16 * 1024 * 1024 + 1025);
    const f = fixture({ raw: oversized, chunkSize: oversized.length });
    expect((yield* f.run().pipe(Effect.result))._tag).toBe("Failure");
    expect(f.output).toEqual([]);
    expect(f.calls).toHaveLength(2);
  }),
);
it.effect("cancellation closes a pending download without stdout or another request", () =>
  Effect.gen(function* () {
    const ready = Promise.withResolvers<void>();
    let cancelled = 0;
    const f = fixture({
      hold: {
        ready: () => ready.resolve(),
        cancelled: () => {
          cancelled++;
        },
      },
    });
    const worker = yield* f.run().pipe(Effect.forkChild);
    yield* Effect.promise(() => ready.promise);
    yield* Fiber.interrupt(worker);
    expect(cancelled).toBe(1);
    expect(f.output).toEqual([]);
    expect(f.calls).toHaveLength(2);
  }),
);
it.effect(
  "missing selected runtime refuses without initializing a key, database or configuration",
  () =>
    Effect.gen(function* () {
      const parent = process.env.NW_NATIVE_EXPORT_CLI_EVIDENCE_DIR ?? NodeOS.tmpdir();
      NodeFS.mkdirSync(parent, { recursive: true, mode: 0o700 });
      const home = NodeFS.mkdtempSync(NodePath.join(parent, "missing-runtime-"));
      let used = 0;
      for (const path of [home, "relative"]) {
        expect(
          (yield* CliSessions.withSelectedCliSessions(path, "read-export", () =>
            Effect.sync(() => {
              used++;
            }),
          ).pipe(Effect.result))._tag,
        ).toBe("Failure");
      }
      expect(used).toBe(0);
      expect(NodeFS.readdirSync(home)).toEqual([]);
    }).pipe(Effect.provide(NodeServices.layer)),
);
