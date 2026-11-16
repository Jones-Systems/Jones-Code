import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Path from "effect/Path";
import type { FleetCheckpointInput } from "@t3tools/contracts/jones/fleetCheckpoint";
import * as Runner from "../../processRunner.ts";
import * as Checkpoints from "./Checkpoint.ts";
import { recipe, placementPolicy } from "./Evidence.ts";
import { canonical, digest } from "./Json.ts";

const configuration = {
  executable: "/qualified/host-tooling",
  arguments: ["fleet-controller"],
  binding: "/qualified/client.json",
  cwd: "/qualified",
  controllerId: "controller",
};
const caller = { environmentId: "environment", threadId: "thread" };
const input: FleetCheckpointInput = {
  originalRequestId: "original-one",
  request: {
    schema: "fleet-ops.checkpoint-placement-request/v1",
    work_key: {
      schema: "fleet-ops.checkpoint-work-key/v1",
      controller_id: "controller",
      admission_epoch: "epoch-1",
    },
    source: {
      schema: "fleet-ops.checkpoint-source/v1",
      repository: "https://github.com/Jones-Systems/host-tooling",
      commit: "a".repeat(40),
      tree: "b".repeat(40),
      manifest_sha256: "c".repeat(64),
      lockset_sha256: "d".repeat(64),
      submodule_sha256: "e".repeat(64),
      lfs_sha256: "f".repeat(64),
    },
    compatibility: {
      platform: { os: "linux", architecture: "x86_64", abi: "glibc" },
      toolchain: [{ name: "python", version: "3.13" }],
      package_manager: { name: "pip", version: "25" },
    },
    recipe: {
      schema: "fleet-ops.checkpoint-recipe/v1",
      recipe_id: "host-tooling.catalog",
      version: "1",
      recipe_digest: digest(recipe),
    },
    registry_revision: "registry-1",
    policy_ref: "policy",
    coverage_ref: "coverage",
    placement_policy_digest: digest(placementPolicy),
    demand: {
      schema: "fleet-ops.checkpoint-demand/v1",
      cpu_units: 1,
      ram_bytes: 1048576,
      output_peak_bytes: 1048576,
      claims: [],
      artifact_max_bytes: 1048576,
    },
    earliest_start: "2026-10-26T00:00:00Z",
    acceptance_deadline: "2026-10-26T00:01:00Z",
    latest_start: "2026-10-26T00:02:00Z",
    prior_costs: [],
  },
};
const output = (value: unknown): Runner.ProcessRunOutput => ({
  stdout: canonical(value),
  stderr: "",
  code: 0 as Runner.ProcessRunOutput["code"],
  timedOut: false,
  stdoutTruncated: false,
  stderrTruncated: false,
  stdoutInvalidUtf8: false,
  stderrInvalidUtf8: false,
});
const absent = (key: unknown) => ({
  schema: "fleet-ops.checkpoint-placement-result/v1",
  work_key: key,
  request_digest: null,
  status: "not-accepted",
  reason: "work-not-accepted",
  selection: null,
  original_result: null,
});
const run = (
  action: Checkpoints.Action,
  value: FleetCheckpointInput,
  runner: Runner.ProcessRunner["Service"],
  who = caller,
) =>
  Effect.flatMap(Checkpoints.Checkpoint, (service) => service.execute(action, who, value)).pipe(
    Effect.provide(Checkpoints.layer(configuration)),
    Effect.provideService(Runner.ProcessRunner, runner),
    Effect.provide(Path.layer),
  );

it.effect(
  "keeps the original work key across lost response, service restart and lookup without retrying placement",
  () =>
    Effect.gen(function* () {
      const calls: Runner.ProcessRunInput[] = [];
      const uncertain = yield* run("place", input, {
        run: (c) => {
          calls.push(c);
          return Effect.succeed({ ...output({}), timedOut: true });
        },
      });
      expect(uncertain.transport).toBe("unknown");
      expect(calls).toHaveLength(1);
      const found = yield* run("get", input, {
        run: (c) => {
          calls.push(c);
          return Effect.succeed(output(absent(JSON.parse(c.stdin!).work_key)));
        },
      });
      expect(found.workKey).toEqual(uncertain.workKey);
      expect(found.verificationOutcome).toBe("unknown");
      expect(calls.map((c) => c.args)).toEqual([
        ["fleet-controller", "place-checkpoint", "--binding", configuration.binding],
        ["fleet-controller", "get-checkpoint", "--binding", configuration.binding],
      ]);
      expect(
        calls.every((c) => c.maxOutputBytes === 262144 && c.timeoutBehavior === "timedOutResult"),
      ).toBe(true);
      const other = yield* run(
        "get",
        input,
        { run: (c) => Effect.succeed(output(absent(JSON.parse(c.stdin!).work_key))) },
        { ...caller, threadId: "another-thread" },
      );
      expect(other.workKey.work_id).not.toBe(found.workKey.work_id);
    }),
);

it.effect(
  "refuses target injection, changed fixed recipe and foreign response identity before presenting evidence",
  () =>
    Effect.gen(function* () {
      for (const invalid of [
        { ...input, request: { ...input.request, target_id: "injected" } },
        {
          ...input,
          request: {
            ...input.request,
            recipe: { ...input.request.recipe, recipe_digest: "0".repeat(64) },
          },
        },
      ]) {
        let calls = 0;
        const result = yield* Effect.exit(
          run("place", invalid, {
            run: () => {
              calls++;
              return Effect.succeed(output({}));
            },
          }),
        );
        expect(Exit.isFailure(result)).toBe(true);
        expect(calls).toBe(0);
      }
      expect(
        Exit.isFailure(
          yield* Effect.exit(
            run("get", input, {
              run: () =>
                Effect.succeed(output(absent({ ...input.request.work_key, work_id: "foreign" }))),
            }),
          ),
        ),
      ).toBe(true);
    }),
);

it.effect(
  "does not turn truncation or nonzero exits into absence and never cancels unaccepted work",
  () =>
    Effect.gen(function* () {
      for (const flags of [
        { stdoutTruncated: true },
        { stdoutInvalidUtf8: true },
        { code: 2 as Runner.ProcessRunOutput["code"] },
      ]) {
        expect(
          (yield* run("place", input, { run: () => Effect.succeed({ ...output({}), ...flags }) }))
            .transport,
        ).toBe("unknown");
      }
      let calls = 0;
      const result = yield* Effect.exit(
        run("cancel-pending", input, {
          run: (c) => {
            calls++;
            return Effect.succeed(output(absent(JSON.parse(c.stdin!).work_key)));
          },
        }),
      );
      expect(Exit.isFailure(result)).toBe(true);
      expect(calls).toBe(1);
    }),
);
