import * as Effect from "effect/Effect";

import type { OrchestratorV2Shape } from "../../orchestration-v2/Orchestrator.ts";

/** The core's two required non-function remote-command ports. */
export type UnusedCorePorts = Pick<
  OrchestratorV2Shape,
  "remoteSendReceipts" | "followUpCommandOwner"
>;

const unexpected = (port: string) => () =>
  Effect.die(
    new Error(
      `Unexpected call to the core port ${port} in this fixture. A test that exercises it needs the real owner on its database.`,
    ),
  );

/**
 * Ports for an `OrchestratorV2` mock whose test never reaches a linked peer.
 * Every operation dies, so an unplanned call fails the test instead of passing silently.
 */
export function unusedCorePorts(): UnusedCorePorts {
  return {
    remoteSendReceipts: {
      available: Effect.succeed(false),
      lookup: unexpected("remoteSendReceipts.lookup"),
      withSendCommit: unexpected("remoteSendReceipts.withSendCommit"),
      pruneBefore: unexpected("remoteSendReceipts.pruneBefore"),
      guardPreparation: unexpected("remoteSendReceipts.guardPreparation"),
    },
    followUpCommandOwner: {
      inParentTransaction: unexpected("followUpCommandOwner.inParentTransaction"),
      assertParentCaller: unexpected("followUpCommandOwner.assertParentCaller"),
      dispatch: unexpected("followUpCommandOwner.dispatch"),
    },
  };
}
