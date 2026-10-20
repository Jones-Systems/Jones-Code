import { it as effectIt } from "@effect/vitest";
import {
  PrimaryConnectionTarget,
  type PreparedConnection,
} from "@t3tools/client-runtime/connection";
import { EnvironmentId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { HttpClient, HttpClientResponse } from "effect/http";
import { describe, expect, it } from "vite-plus/test";

import {
  installedComparisonFor,
  installedSourceAncestry,
  installedSourcePair,
  readInstalledSourceAncestry,
} from "./installedSourceObservation";

const REPOSITORY = "Jones-Systems/Jones-Code";
const HOST = "a".repeat(40),
  LAPTOP = "b".repeat(40),
  OTHER = "c".repeat(40);
const host = { repository: REPOSITORY, sha: HOST };
const laptop = { repository: REPOSITORY, sha: LAPTOP };
const pair = { base: HOST, head: LAPTOP };
const report = (relation: "ahead" | "behind" | "identical" | "diverged" | "unknown") => ({
  repository: REPOSITORY,
  base: HOST,
  head: LAPTOP,
  relation,
  observedAt: "2026-10-20T12:00:00.000Z",
});

describe("installed source pair", () => {
  it("queries only distinct full commits from the fixed repository, host as base", () => {
    expect(installedSourcePair(host, laptop)).toEqual(pair);
    expect(installedSourcePair(host, { ...laptop, sha: HOST })).toBeUndefined();
    expect(installedSourcePair(undefined, laptop)).toBeUndefined();
    expect(installedSourcePair(host, undefined)).toBeUndefined();
    expect(installedSourcePair({ ...host, sha: HOST.slice(0, 7) }, laptop)).toBeUndefined();
    expect(installedSourcePair({ ...host, repository: "someone/fork" }, laptop)).toBeUndefined();
  });
});

describe("installed source ancestry", () => {
  it.each([
    ["ahead", "older"],
    ["behind", "newer"],
    ["diverged", "diverged"],
  ] as const)("maps the server's %s to an installed %s badge", (relation, badge) => {
    const ancestry = installedSourceAncestry(pair, report(relation));
    expect(ancestry).toEqual({ base: HOST, head: LAPTOP, relation });
    expect(installedComparisonFor({ host, laptop, ancestry })).toBe(badge);
  });

  it("keeps distinct commits as a different source without a proven order", () => {
    for (const relation of ["identical", "unknown"] as const)
      expect(installedSourceAncestry(pair, report(relation))).toBeUndefined();
    expect(installedComparisonFor({ host, laptop, ancestry: undefined })).toBe("different");
  });

  it("refuses a report for another pair, the swapped pair, or another repository", () => {
    expect(installedSourceAncestry(pair, { ...report("ahead"), head: OTHER })).toBeUndefined();
    expect(
      installedSourceAncestry(pair, { ...report("ahead"), base: LAPTOP, head: HOST }),
    ).toBeUndefined();
    expect(
      installedSourceAncestry(pair, { ...report("ahead"), repository: "someone/fork" }),
    ).toBeUndefined();
    expect(installedSourceAncestry(pair, undefined)).toBeUndefined();
  });

  it("ignores ancestry proven for a different pair than the one displayed", () => {
    const stale = { base: HOST, head: OTHER, relation: "ahead" as const };
    expect(installedComparisonFor({ host, laptop, ancestry: stale })).toBe("different");
  });

  it("matches one commit without ancestry, even across different versions", () => {
    expect(
      installedComparisonFor({ host, laptop: { ...laptop, sha: HOST }, ancestry: "loading" }),
    ).toBe("matches");
  });

  it("holds the badge while loading and reports unavailable without both receipts", () => {
    expect(installedComparisonFor({ host, laptop, ancestry: "loading" })).toBeNull();
    expect(installedComparisonFor({ host: undefined, laptop, ancestry: "loading" })).toBe(
      "unavailable",
    );
    expect(installedComparisonFor({ host, laptop: undefined, ancestry: undefined })).toBe(
      "unavailable",
    );
  });
});

describe("reading ancestry from this laptop's server", () => {
  const prepared: PreparedConnection = {
    environmentId: EnvironmentId.make("local-laptop"),
    label: "This laptop",
    httpBaseUrl: "http://127.0.0.1:3773",
    socketUrl: "ws://127.0.0.1:3773/ws",
    httpAuthorization: null,
    target: new PrimaryConnectionTarget({
      environmentId: EnvironmentId.make("local-laptop"),
      label: "This laptop",
      httpBaseUrl: "http://127.0.0.1:3773",
      wsBaseUrl: "ws://127.0.0.1:3773",
    }),
  };
  function read(status: number, body: unknown) {
    const urls: string[] = [];
    const client = HttpClient.make((request, url) =>
      Effect.sync(() => {
        urls.push(url.toString());
        return HttpClientResponse.fromWeb(
          request,
          new Response(JSON.stringify(body), {
            status,
            headers: { "content-type": "application/json" },
          }),
        );
      }),
    );
    return readInstalledSourceAncestry(prepared, pair).pipe(
      Effect.provideService(HttpClient.HttpClient, client),
      Effect.map((ancestry) => ({ ancestry, urls })),
    );
  }

  effectIt.effect("requests the exact pair and accepts the bound answer", () =>
    Effect.gen(function* () {
      const { ancestry, urls } = yield* read(200, report("ahead"));
      expect(ancestry).toEqual({ base: HOST, head: LAPTOP, relation: "ahead" });
      expect(urls).toHaveLength(1);
      const url = new URL(urls[0]!);
      expect(url.pathname).toBe("/api/jones-updates/source-relation");
      expect(url.searchParams.get("base")).toBe(HOST);
      expect(url.searchParams.get("head")).toBe(LAPTOP);
    }),
  );

  effectIt.effect(
    "falls back to an unknown order when this laptop's server predates the route",
    () =>
      Effect.gen(function* () {
        expect((yield* read(404, { error: "not found" })).ancestry).toBeUndefined();
      }),
  );

  effectIt.effect("falls back to an unknown order for a malformed or mismatched answer", () =>
    Effect.gen(function* () {
      expect(
        (yield* read(200, { ...report("ahead"), relation: "older" })).ancestry,
      ).toBeUndefined();
      expect((yield* read(200, { ...report("ahead"), head: OTHER })).ancestry).toBeUndefined();
    }),
  );
});
