import { it } from "vite-plus/test";
import { nativePlatform } from "./Filesystem.ts";
import {
  darwinNativeObservationCases,
  nativeObservationCases,
} from "./NativeObservation.testkit.ts";
it.each(Object.entries(nativeObservationCases))("%s", (_name, run) => run());
it.skipIf(nativePlatform !== "darwin").each(Object.entries(darwinNativeObservationCases))(
  "%s",
  (_name, run) => run(),
);
