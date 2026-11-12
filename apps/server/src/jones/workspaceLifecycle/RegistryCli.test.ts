import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Runner from "../../processRunner.ts";
import * as Registry from "./RegistryCli.ts";
import { Binding } from "./Protocol.ts";
import { canonical } from "../ordinaryCheckout/CheckoutProtocol.ts";
const decodeBinding = Schema.decodeEffect(Schema.fromJsonString(Binding));

const binding: Binding = {
  environment_id: "environment",
  thread_id: "thread",
  incarnation_id: "birth-and-physical-identity",
  repository: "Jones-Systems/Synthetic",
  worktree: "/repo-linked",
  git_common_dir: "/repo/.git",
  git_dir: "/repo/.git/worktrees/linked",
  branch: "refs/heads/work",
  head: "a".repeat(40),
  source_id: "original-native-source",
  source_generation: "original-transition-token",
};
const configuration: Registry.RegistryCommand = {
  executable: "/qualified/v3",
  arguments: [],
  database: "/existing/registry.sqlite",
  cwd: "/qualified",
};
const output = (stdout: string, code = 0): Runner.ProcessRunOutput => ({
  stdout,
  stderr: "",
  code: code as Runner.ProcessRunOutput["code"],
  timedOut: false,
  stdoutTruncated: false,
  stderrTruncated: false,
  stdoutInvalidUtf8: false,
  stderrInvalidUtf8: false,
});
const inventory = {
  schema: "workspace-lifecycle-cli/v1",
  command: "show",
  result: {
    schema: "workspace-inventory/v1",
    outcome: "unknown",
    next_after: null,
    evidence: "stored_only",
    authority_effect: "none",
    results: [
      {
        worktree: binding.worktree,
        generation: "cas-generation",
        binding,
        assessment: null,
        disposition: "unknown",
        assessment_status: "absent",
        stored_readiness: null,
        physical_readiness: "unknown",
        binding_evidence: "caller_asserted",
        authority_effect: "none",
      },
    ],
  },
};

it.effect(
  "consumes the frozen read-only inventory and bounded stdin/CAS resume CLI without setup commands",
  () =>
    Effect.gen(function* () {
      const calls: Runner.ProcessRunInput[] = [];
      const runner = Runner.ProcessRunner.of({
        run: (input) =>
          Effect.sync(() => {
            calls.push(input);
            return input.args.includes("show")
              ? output(canonical(inventory), 1)
              : output(
                  canonical({
                    schema: "workspace-lifecycle-cli/v1",
                    command: "resume",
                    result: {
                      schema: "workspace-write/v1",
                      outcome: "resumed",
                      generation: "successor-cas",
                      authority_effect: "none",
                    },
                  }),
                );
          }),
      });
      yield* Effect.gen(function* () {
        const registry = yield* Registry.LifecycleRegistry;
        expect((yield* registry.show(binding.worktree)).binding).toEqual(binding);
        expect(yield* registry.resume(binding, "cas-generation")).toBe("successor-cas");
      }).pipe(
        Effect.provide(Registry.layer(configuration)),
        Effect.provideService(Runner.ProcessRunner, runner),
        Effect.provide(Path.layer),
      );
      expect(calls.map((c) => c.args[2])).toEqual(["show", "resume"]);
      expect(calls[1]!.args).toEqual([
        "--database",
        configuration.database,
        "resume",
        "--input",
        "-",
        "--expected-generation",
        "cas-generation",
      ]);
      expect(yield* decodeBinding(calls[1]!.stdin!)).toEqual(binding);
      expect(
        calls.every((c) => c.maxOutputBytes === 256 * 1024 && c.timeout === "10 seconds"),
      ).toBe(true);
    }),
);

it.effect(
  "holds malformed, truncated, foreign-path and unavailable inventories instead of treating them as an absent row",
  () =>
    Effect.gen(function* () {
      for (const result of [
        output("not JSON"),
        { ...output(canonical(inventory)), stdoutTruncated: true },
        output(canonical({ ...inventory, result: { ...inventory.result, results: [] } }), 1),
        output(
          canonical({
            ...inventory,
            result: {
              ...inventory.result,
              results: [{ ...inventory.result.results[0], worktree: "/foreign" }],
            },
          }),
        ),
        output(canonical(inventory), 2),
      ]) {
        const program = Effect.flatMap(Registry.LifecycleRegistry, (registry) =>
          registry.show(binding.worktree),
        ).pipe(
          Effect.provide(Registry.layer(configuration)),
          Effect.provideService(Runner.ProcessRunner, { run: () => Effect.succeed(result) }),
          Effect.provide(Path.layer),
        );
        expect(Exit.isFailure(yield* Effect.exit(program))).toBe(true);
      }
    }),
);
