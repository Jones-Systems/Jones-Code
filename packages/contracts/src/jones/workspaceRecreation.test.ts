import { it, expect } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { OrchestrationMessageContext } from "../composerContext.ts";
import { CheckoutRecreationRequest } from "./workspaceRecreation.ts";
import { ExecutionEnvironmentCapabilities } from "../environment.ts";

const decodeMessageContext = Schema.decodeSync(OrchestrationMessageContext);
const decodeRecreationRequest = Schema.decodeUnknownSync(CheckoutRecreationRequest);

it("preserves explicit receipt-bound detached requests through message-context replay and keeps old messages valid", () => {
  const context = {
    version: 1 as const,
    records: [],
    checkoutRecreation: { receiptId: "receipt", mode: "detached" as const },
  };
  const codec = Schema.fromJsonString(OrchestrationMessageContext);
  expect(Schema.decodeSync(codec)(Schema.encodeSync(codec)(context))).toEqual(context);
  expect(decodeMessageContext({ version: 1, records: [] })).toEqual({
    version: 1,
    records: [],
  });
  for (const request of [
    { receiptId: "", mode: "detached" },
    { receiptId: "receipt", mode: "reset" },
  ])
    expect(() => decodeRecreationRequest(request)).toThrow();
  const decodeCapabilities = Schema.decodeUnknownSync(ExecutionEnvironmentCapabilities);
  expect(decodeCapabilities({}).checkoutRecreation).toBeUndefined();
  expect(decodeCapabilities({ checkoutRecreation: true }).checkoutRecreation).toBe(true);
});
