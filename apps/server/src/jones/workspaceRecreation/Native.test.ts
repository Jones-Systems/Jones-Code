import { it } from "vite-plus/test";
import { nativeCases } from "./Native.testkit.ts";
it.each(Object.entries(nativeCases))("%s", (_name, run) => run());
