import { after, test } from "node:test";
import { nativeCases, scratchReceipts } from "./Native.testkit.ts";
for (const [name, run] of Object.entries(nativeCases)) test(name, run);
after((context) => {
  for (const receipt of scratchReceipts) context.diagnostic(JSON.stringify(receipt));
});
