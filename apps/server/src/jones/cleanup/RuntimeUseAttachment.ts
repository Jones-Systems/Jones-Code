import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import { RuntimeUseError } from "./RuntimeUseProtocol.ts";

interface ExpectedAttachment {
  readonly threadId: string;
  readonly providerSessionId: string;
  readonly validate: Effect.Effect<void, RuntimeUseError>;
  readonly revalidate: Effect.Effect<void, RuntimeUseError>;
}
class AttachmentPublication extends Context.Service<
  AttachmentPublication,
  ExpectedAttachment & { readonly active: () => boolean }
>()("t3/jones/cleanup/RuntimeUseAttachment/AttachmentPublication") {}

/** Adds validation to the real EventSink transaction; never supplies a native
 * permit or replaces the ordinary attachment/authorization checks. */
export const withRuntimeUseAttachmentPublication = <A, E, R>(
  expected: ExpectedAttachment,
  effect: Effect.Effect<A, E, R>,
) =>
  Effect.suspend(() => {
    let active = true;
    return effect.pipe(
      Effect.provideService(AttachmentPublication, { ...expected, active: () => active }),
      Effect.ensuring(
        Effect.sync(() => {
          active = false;
        }),
      ),
    );
  });

const expectedPublication = Effect.fn("RuntimeUseAttachment.expectedPublication")(function* (
  threadId: string,
  providerSessionId: string,
) {
  const expected = yield* Effect.serviceOption(AttachmentPublication);
  if (Option.isNone(expected)) return;
  if (
    !expected.value.active() ||
    expected.value.threadId !== threadId ||
    expected.value.providerSessionId !== providerSessionId
  )
    return yield* new RuntimeUseError({
      reason: "runtime_use_attachment_publication_owner_changed",
    });
  return expected.value;
});

// Physical/owner observations stay outside the application SQL transaction.
// The transaction below independently repeats the expected persisted fence.
export const revalidateRuntimeUseAttachmentBeforePublication = Effect.fn(
  "RuntimeUseAttachment.revalidateBeforePublication",
)(function* (threadId: string, providerSessionId: string) {
  const expected = yield* expectedPublication(threadId, providerSessionId);
  if (expected === undefined) return;
  const sql = yield* SqlClient.SqlClient;
  if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
    return yield* new RuntimeUseError({
      reason: "runtime_use_attachment_physical_check_inside_transaction",
    });
  yield* expected.revalidate;
});

export const validateRuntimeUseAttachmentPublication = Effect.fn(
  "RuntimeUseAttachment.validatePublication",
)(function* (threadId: string, providerSessionId: string) {
  const expected = yield* expectedPublication(threadId, providerSessionId);
  if (expected === undefined) return;
  const sql = yield* SqlClient.SqlClient;
  if (Option.isNone(yield* Effect.serviceOption(sql.transactionService)))
    return yield* new RuntimeUseError({
      reason: "runtime_use_attachment_publication_outside_transaction",
    });
  yield* expected.validate;
});
