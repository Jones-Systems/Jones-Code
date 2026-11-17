const { test } = await import("node:test");
const { reftableReaderCases } = await import("./ReftableObservation.testkit.ts");
for (const [name, run] of Object.entries(reftableReaderCases)) test(name, run);
