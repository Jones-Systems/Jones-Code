import type { OrchestrationMessageContext } from "@t3tools/contracts";
import { serializeLegacyContextMessage } from "@t3tools/shared/composerContextLegacySend";
export function withMessageContext(input: {
  readonly text: string;
  readonly context: OrchestrationMessageContext | undefined;
  readonly supportsInlineMessageContext?: boolean;
}): { readonly text: string; readonly context?: OrchestrationMessageContext } {
  const { text, context } = input;
  if (context === undefined) return { text };
  if (input.supportsInlineMessageContext === false)
    return { text: serializeLegacyContextMessage({ text, records: context.records }) };
  return { text, context };
}
