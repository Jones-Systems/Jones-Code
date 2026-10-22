import * as NodeTest from "node:test";
import * as NodeAssert from "node:assert/strict";
import { generationProgress, launchWithProgress, restartWithProgress } from "./runner.mjs";

function fixture(fault) {
  const records = [],
    operations = [],
    generations = [{ index: 0 }];
  const progress = generationProgress("/synthetic", async (path, bytes, options) => {
    NodeAssert.equal(path, "/synthetic/restart-progress.ndjson");
    NodeAssert.equal(options.flush, true);
    NodeAssert.equal(options.mode, 0o600);
    records.push(JSON.parse(bytes));
  });
  const operation = async (phase) => {
    operations.push(phase);
    NodeAssert.equal(records.at(-1).phase, phase);
    NodeAssert.equal(records.at(-1).state, phase === "launch" ? "requested" : "entered");
    if (fault === phase) throw new Error("private-native-error");
  };
  const launch = () =>
    launchWithProgress({
      index: generations.length,
      progress,
      launch: async () => {
        await operation("launch");
        const generation = { index: generations.length };
        generations.push(generation);
        return generation;
      },
      window: () => operation("window"),
      bridge: () => operation("bridge"),
      setup: async (generation) => {
        await operation("setup");
        return { generation };
      },
    });
  const run = () =>
    restartWithProgress({
      previous: generations[0],
      progress,
      identify: (generation) =>
        operation(generation.index === 0 ? "identify-before-close" : "identity"),
      close: () => operation("close"),
      diagnostics: () => operation("diagnostics"),
      launch,
      assertIdentity: () => {
        if (fault === "identity-validation") throw new Error("private-native-error");
      },
    });
  return { records, operations, generations, run };
}

NodeTest.test(
  "each restart boundary preserves entered progress without fabricating completion",
  async () => {
    const phases = [
      "identify-before-close",
      "close",
      "diagnostics",
      "launch",
      "window",
      "bridge",
      "setup",
      "identity",
    ];
    for (const fault of [...phases, "identity-validation"]) {
      const f = fixture(fault);
      await NodeAssert.rejects(f.run(), /private-native-error/);
      const last = fault === "identity-validation" ? "identity" : fault;
      NodeAssert.deepEqual(f.operations, phases.slice(0, phases.indexOf(last) + 1));
      NodeAssert.deepEqual(f.records.at(-1), {
        generation: phases.indexOf(last) < 3 ? 0 : 1,
        phase: last,
        state: last === "launch" ? "requested" : "entered",
      });
      NodeAssert.equal(f.generations.length, phases.indexOf(last) <= 3 ? 1 : 2);
      NodeAssert.equal(JSON.stringify(f.records).includes("private-native-error"), false);
      NodeAssert.equal(f.records.length, phases.indexOf(last) * 2 + 1);
    }
  },
);

NodeTest.test(
  "successful restart durably completes each boundary and validates replacement identity last",
  async () => {
    const f = fixture();
    const replacement = await f.run();
    NodeAssert.equal(replacement.generation, f.generations[1]);
    NodeAssert.equal(f.records.length, 16);
    for (let i = 0; i < f.records.length; i += 2) {
      const entered = f.records[i],
        completed = f.records[i + 1];
      NodeAssert.deepEqual(completed, {
        ...entered,
        state: entered.phase === "launch" ? "returned" : "completed",
      });
    }
    NodeAssert.deepEqual(f.records.at(-1), { generation: 1, phase: "identity", state: "completed" });
  },
);

NodeTest.test("progress persistence failure prevents the covered operation", async () => {
  let calls = 0;
  const progress = generationProgress("/synthetic", async () => {
    throw new Error("write-failed");
  });
  await NodeAssert.rejects(
    progress(1, "launch", () => {
      calls++;
    }),
    /write-failed/,
  );
  NodeAssert.equal(calls, 0);
  await NodeAssert.rejects(
    progress(1, "raw-private-error", () => {
      calls++;
    }),
    /progress-invalid/,
  );
  NodeAssert.equal(calls, 0);
});

NodeTest.test(
  "pending launch records requested without claiming a returned or spawned generation",
  async () => {
    const records = [];
    let entered;
    const enteredPromise = new Promise((resolve) => {
      entered = resolve;
    });
    const progress = generationProgress("/synthetic", async (_path, bytes) => {
      records.push(JSON.parse(bytes));
    });
    let release;
    const pending = progress(1, "launch", () => {
      entered();
      return new Promise((resolve) => {
        release = resolve;
      });
    });
    await enteredPromise;
    NodeAssert.deepEqual(records, [{ generation: 1, phase: "launch", state: "requested" }]);
    release("owned-generation");
    NodeAssert.equal(await pending, "owned-generation");
    NodeAssert.deepEqual(records.at(-1), { generation: 1, phase: "launch", state: "returned" });
  },
);
