import { installEffectSeam } from "./EffectSeam.node-test-support.mjs";
installEffectSeam();
const { test } = await import("node:test");
const { nativePlatform } = await import("./Filesystem.ts");
const { darwinNativeObservationCases, nativeObservationCases } =
  await import("./NativeObservation.testkit.ts");
for (const [name, run] of Object.entries(nativeObservationCases)) test(name, run);
for (const [name, run] of Object.entries(darwinNativeObservationCases))
  test(name, { skip: nativePlatform !== "darwin" && "Darwin-only native observation" }, run);
