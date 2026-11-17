const { test } = await import("node:test");
const { qualificationCases, recoverabilityCases, reftableAvailable, reftableCases } =
  await import("./Qualification.testkit.ts");
for (const [name, run] of Object.entries(qualificationCases)) test(name, run);
for (const [name, run] of Object.entries(recoverabilityCases)) test(name, run);
for (const [name, run] of Object.entries(reftableCases))
  test(name, { skip: reftableAvailable ? false : "installed Git lacks --ref-format" }, run);
