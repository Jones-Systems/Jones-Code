const { test } = await import("node:test");
const { qualificationCases } = await import("./Qualification.testkit.ts");
for (const [name, run] of Object.entries(qualificationCases)) test(name, run);
