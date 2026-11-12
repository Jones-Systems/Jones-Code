import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Owner from "./setupOwner.ts";
import * as Setup from "./peerSetup.ts";
import type * as Mcp from "../../../../mcp/McpInvocationContext.ts";
it.effect(
  "JSON/copied private witnesses and copied operation handles cannot produce a qualifier",
  () =>
    Effect.gen(function* () {
      expect(
        yield* Setup.makePeerSetupQualification(
          {} as Owner.QualifiedSetupOperation,
          {} as Mcp.McpInvocationScope,
          {} as Setup.VerifiedSetupExchange,
        ).pipe(Effect.result),
      ).toMatchObject({ _tag: "Failure", failure: { code: "owner_unqualified" } });
    }),
);
