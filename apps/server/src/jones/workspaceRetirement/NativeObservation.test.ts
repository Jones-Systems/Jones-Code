import { it } from "vite-plus/test";
import { nativeObservationCases } from "./NativeObservation.testkit.ts";
it.each(Object.entries(nativeObservationCases))("%s", (_name, run) => run());
