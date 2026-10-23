import * as NodeAssert from "node:assert/strict";
import * as NodeFS from "node:fs";
import * as NodeChildProcess from "node:child_process";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";

const workflow = NodeFS.readFileSync(
  new URL("../../../.github/workflows/ci.yml", import.meta.url),
  "utf8",
);

NodeTest.test("CI keeps Chromium apt work conditional and one apt update per test job", () => {
  NodeAssert.doesNotMatch(workflow, /install --with-deps --only-shell chromium/);
  for (const jobName of ["test", "test_desktop"]) {
    const packageJob = workflow.split(`\n  ${jobName}:\n`)[1]?.split(/\n  [\w-]+:\n/)[0];
    NodeAssert.ok(packageJob, `Missing CI job: ${jobName}`);
    NodeAssert.equal(packageJob.match(/sudo apt-get update/g)?.length, 1, jobName);
    NodeAssert.match(packageJob, /build-essential libsecret-1-dev pkg-config/);
    NodeAssert.match(packageJob, /command -v cc/);
  }
});

const { ensureChromium, resolvePlaywright } = await import("./chromium-setup.mjs");

function scenario(failures, repairError) {
  const events = [];
  let launches = 0;
  return {
    events,
    options: {
      chromium: {
        async launch(options) {
          NodeAssert.deepEqual(options, { headless: true, timeout: 30_000 });
          events.push("launch");
          if (launches++ < failures) throw new Error("browser unavailable");
          return {
            async close() {
              events.push("close");
            },
          };
        },
      },
      async installDependencies() {
        events.push("install-deps");
        if (repairError) throw repairError;
      },
      report() {
        events.push("diagnostic");
      },
    },
  };
}

NodeTest.test("a working browser closes without installing system dependencies", async () => {
  const { events, options } = scenario(0);
  await ensureChromium(options);
  NodeAssert.deepEqual(events, ["launch", "close"]);
});

NodeTest.test("failed launch repairs once and verifies a real second launch", async () => {
  const { events, options } = scenario(1);
  await ensureChromium(options);
  NodeAssert.deepEqual(events, ["launch", "diagnostic", "install-deps", "launch", "close"]);
});

NodeTest.test("an unsuccessful repair remains a failure without looping", async () => {
  const { events, options } = scenario(2);
  await NodeAssert.rejects(ensureChromium(options), /browser unavailable/);
  NodeAssert.deepEqual(events, ["launch", "diagnostic", "install-deps", "launch"]);
});

NodeTest.test("installer failure is preserved without attempting another browser", async () => {
  const failure = new Error("installer failed");
  const { events, options } = scenario(1, failure);
  await NodeAssert.rejects(ensureChromium(options), (error) => error === failure);
  NodeAssert.deepEqual(events, ["launch", "diagnostic", "install-deps"]);
});

NodeTest.test("unsupported workspaces fail before loading a package", () => {
  NodeAssert.throws(() => resolvePlaywright("apps/other"), /Unsupported Chromium workspace/);
});

function workflowJob(name) {
  const job = workflow.split(`\n  ${name}:\n`)[1]?.split(/\n  [\w-]+:\n/)[0];
  NodeAssert.ok(job, `Missing CI job: ${name}`);
  return job;
}

function workflowStepScript(jobName, stepName) {
  const step = workflowJob(jobName).split(`      - name: ${stepName}\n`)[1]?.split("\n      - ")[0];
  const script = step?.split("        run: |\n")[1];
  NodeAssert.ok(script, `Missing run script: ${jobName}/${stepName}`);
  return script.replace(/^ {10}/gm, "");
}

// These specific expressions use the Boolean/string subset shared with JavaScript.
function workflowExpression(expression, event, scope, nativeOutputs = {}) {
  NodeAssert.match(expression, /^\$\{\{ .+ \}\}$/);
  return Function(
    "github",
    "inputs",
    "needs",
    "cancelled",
    `return (${expression.slice(3, -2)});`,
  )(
    { event_name: event },
    { measurement_scope: scope },
    { native_changes: { outputs: nativeOutputs } },
    () => false,
  );
}

NodeTest.test(
  "only explicit manual Linux measurements change qualification names and native gates",
  () => {
    const scopeExpression = workflow.match(/^  CI_MEASUREMENT_SCOPE: (.+)$/m)?.[1];
    NodeAssert.ok(scopeExpression);
    NodeAssert.match(
      workflow,
      /measurement_scope:\n[\s\S]*?options:\n          - full\n          - linux-measurement\n        default: full/,
    );
    NodeAssert.match(
      workflow,
      /format\('dispatch-\{0\}-\{1\}-\{2\}-\{3\}-\{4\}'[^\n]+inputs.measurement_scope\)/,
    );
    const jobs = [
      ["mobile_native_static_analysis", "Mobile Native Static Analysis", "mobile"],
      [
        "native_enrollment_qualification_macos",
        "Native Enrollment (macos-15, Node 26.8.2)",
        "enrollment",
      ],
    ];
    for (const event of ["pull_request", "push", "workflow_dispatch"]) {
      for (const scope of [undefined, "", "full", "unknown", "linux-measurement"]) {
        const partial = event === "workflow_dispatch" && scope === "linux-measurement";
        NodeAssert.equal(
          workflowExpression(scopeExpression, event, scope),
          partial ? "linux-measurement" : "full",
        );
        const checkName = workflowJob("check").match(/^    name: (.+)$/m)[1];
        NodeAssert.equal(
          workflowExpression(checkName, event, scope),
          partial ? "Linux measurement (partial)" : "Check",
        );
        for (const [id, fullName, output] of jobs) {
          const job = workflowJob(id);
          const name = workflowExpression(job.match(/^    name: (.+)$/m)[1], event, scope);
          if (partial) NodeAssert.match(name, /omitted for Linux measurement/);
          else NodeAssert.equal(name, fullName);
          const condition = job.match(/^    if: (.+)$/m)[1];
          NodeAssert.match(condition, /!cancelled\(\)/);
          for (const value of [undefined, "true", "false"]) {
            NodeAssert.equal(
              workflowExpression(condition, event, scope, { [output]: value }),
              !partial && value !== "false",
            );
          }
        }
      }
    }
    const allJobs = [...workflow.split("\njobs:\n")[1].matchAll(/^  ([\w-]+):\n/gm)]
      .map((match) => match[1])
      .filter((name) => name !== "check");
    const required = [...workflowJob("check").matchAll(/^        ([\w-]+),$/gm)].map(
      (match) => match[1],
    );
    NodeAssert.deepEqual(required.toSorted(), allJobs.toSorted());
  },
);

NodeTest.test(
  "actual aggregation filter accepts only qualified full or explicitly partial results",
  () => {
    const script = workflowStepScript("check", "Require every job to pass");
    const jobNames = [...workflowJob("check").matchAll(/^        ([\w-]+),$/gm)].map(
      (match) => match[1],
    );
    const macJobs = ["mobile_native_static_analysis", "native_enrollment_qualification_macos"];
    const results = () =>
      Object.fromEntries(jobNames.map((name) => [name, { result: "success", outputs: {} }]));
    const run = (scope, input) =>
      NodeChildProcess.spawnSync(
        "bash",
        ["--noprofile", "--norc", "-e", "-o", "pipefail", "-c", script],
        {
          encoding: "utf8",
          env: { ...process.env, CI_MEASUREMENT_SCOPE: scope, RESULTS: JSON.stringify(input) },
        },
      );
    const assertResult = (label, scope, input, passes) => {
      const result = run(scope, input);
      NodeAssert.equal(result.status === 0, passes, `${label}: ${result.stderr}`);
      NodeAssert.notEqual(result.status, null, label);
    };
    assertResult("full success", "full", results(), true);
    const unchanged = results();
    unchanged.native_changes.outputs = { mobile: "false", enrollment: "false" };
    for (const id of macJobs) unchanged[id].result = "skipped";
    assertResult("full unchanged native skips", "full", unchanged, true);
    const partial = results();
    for (const id of macJobs) partial[id].result = "skipped";
    assertResult("partial exact omissions", "linux-measurement", partial, true);
    assertResult("full cannot omit changed native jobs", "full", partial, false);
    assertResult("unknown scope stays full", "unknown", partial, false);
    assertResult("partial cannot mislabel full execution", "linux-measurement", results(), false);
    for (const scope of ["full", "linux-measurement"]) {
      for (const outcome of ["failure", "cancelled", "skipped"]) {
        const input = structuredClone(scope === "full" ? results() : partial);
        input.test_server.result = outcome;
        assertResult(`${scope} Linux ${outcome}`, scope, input, false);
      }
      const input = structuredClone(scope === "full" ? results() : partial);
      input.native_changes.result = "failure";
      assertResult(`${scope} classifier failure`, scope, input, false);
    }
    for (const id of macJobs) {
      const input = structuredClone(partial);
      input[id].result = "cancelled";
      assertResult(`partial cancelled ${id}`, "linux-measurement", input, false);
    }
  },
);

NodeTest.test(
  "conflicting Linux measurement and forced native input fails before force or API work",
  () => {
    const script = workflowStepScript("native_changes", "Detect native changes");
    const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "ci-measurement-"));
    try {
      const output = NodePath.join(directory, "outputs");
      for (const scope of ["linux-measurement", "full"]) {
        NodeFS.writeFileSync(output, "");
        const result = NodeChildProcess.spawnSync(
          "bash",
          ["--noprofile", "--norc", "-e", "-o", "pipefail", "-c", script],
          {
            encoding: "utf8",
            env: {
              ...process.env,
              CI_MEASUREMENT_SCOPE: scope,
              FORCE_NATIVE: "true",
              GITHUB_OUTPUT: output,
            },
          },
        );
        if (scope === "linux-measurement") {
          NodeAssert.equal(result.status, 1, result.stderr);
          NodeAssert.match(result.stderr, /cannot be combined with native_jobs=true/);
          NodeAssert.equal(NodeFS.readFileSync(output, "utf8"), "");
        } else {
          NodeAssert.equal(result.status, 0, result.stderr);
          NodeAssert.equal(NodeFS.readFileSync(output, "utf8"), "mobile=true\nenrollment=true\n");
        }
      }
    } finally {
      NodeFS.rmSync(directory, { recursive: true, force: true });
    }
  },
);
