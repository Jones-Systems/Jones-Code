const LINUX_GATES = {
  typecheck: "typecheck",
  build: "build",
  test_web: "test_web",
  test_desktop: "test_desktop",
  test_mobile: "test_mobile",
  test_client_runtime: "test_client_runtime",
  server_weights: "test_server",
  test_server: "test_server",
  "transfer-report": "test_server",
  server_timings: "test_server",
  rust: "rust",
  release_smoke: "release_smoke",
};
const NATIVE_GATES = {
  mobile_native_static_analysis: "mobile",
  native_enrollment_qualification_macos: "enrollment",
};
const REQUIRED_JOBS = [
  "lint",
  "test",
  "linux_changes",
  "native_changes",
  "native_enrollment_qualification",
  ...Object.keys(LINUX_GATES),
  ...Object.keys(NATIVE_GATES),
];

function failures(results) {
  const failed = new Set(REQUIRED_JOBS.filter((name) => !results[name]));
  for (const [classifier, gates] of [
    ["linux_changes", LINUX_GATES],
    ["native_changes", NATIVE_GATES],
  ]) {
    if (
      Object.values(gates).some(
        (output) => !["true", "false"].includes(results[classifier]?.outputs?.[output]),
      )
    ) {
      failed.add(classifier);
    }
  }
  for (const [name, job] of Object.entries(results)) {
    if (job.result === "success") continue;
    const classifier = LINUX_GATES[name]
      ? "linux_changes"
      : NATIVE_GATES[name]
        ? "native_changes"
        : null;
    const output = LINUX_GATES[name] ?? NATIVE_GATES[name];
    if (
      job.result === "skipped" &&
      classifier &&
      results[classifier]?.result === "success" &&
      results[classifier]?.outputs?.[output] === "false"
    )
      continue;
    failed.add(name);
  }
  return [...failed];
}

function main() {
  const results = JSON.parse(process.env.RESULTS);
  for (const [name, job] of Object.entries(results)) console.log(`${name}: ${job.result}`);
  const failed = failures(results);
  if (failed.length) {
    console.error(`Required jobs did not pass: ${failed.join(", ")}`);
    process.exitCode = 1;
  }
}

module.exports = { failures, LINUX_GATES, NATIVE_GATES, REQUIRED_JOBS };
if (require.main === module) main();
