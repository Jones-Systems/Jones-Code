import * as NodeTest from "node:test";
import { nativeCases, scratchReceipts } from "./Native.testkit.ts";
for (const [name, run] of Object.entries(nativeCases)) NodeTest.test(name, run);
NodeTest.after((context) => {
  for (const receipt of scratchReceipts) context.diagnostic(JSON.stringify(receipt));
});
