import * as Schema from "effect/Schema";
import { ThreadId } from "../baseSchemas.ts";
import { ThreadReferenceExcerpt } from "./threadReferenceExcerpt.ts";

export const ThreadReferenceExcerptInput = Schema.Struct({ threadId: ThreadId });
export const ThreadReferenceExcerptResult = Schema.Struct({
  threadId: ThreadId,
  excerpt: ThreadReferenceExcerpt,
});
