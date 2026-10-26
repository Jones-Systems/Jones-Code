import * as Schema from "effect/Schema";
import * as SchemaTransformation from "effect/SchemaTransformation";

export type ThreadTitleVerdict = "sufficient" | "insufficient" | "unknown";

const assessmentResponse = Schema.Struct({
  title: Schema.String,
  needsRefinement: Schema.optional(Schema.Unknown),
  grounded: Schema.optional(Schema.Unknown),
  specific: Schema.optional(Schema.Unknown),
  recognizable: Schema.optional(Schema.Unknown),
});

const assessmentGeneration = Schema.Struct({
  title: Schema.String,
  needsRefinement: Schema.Boolean,
  grounded: Schema.Boolean,
  specific: Schema.Boolean,
  recognizable: Schema.Boolean,
});

// Provider JSON Schema projection requires typed, required fields. Response decoding
// remains tolerant: missing or malformed quality consumes an opportunity without a pass.
export const assessedThreadTitleSchema = Schema.declare(Schema.is(assessmentResponse), {
  toCodecJson: () =>
    Schema.link<typeof assessmentResponse.Type>()(
      assessmentGeneration,
      SchemaTransformation.passthrough({ strict: false }),
    ),
});

export function threadTitleVerdict(result: {
  readonly grounded?: unknown;
  readonly specific?: unknown;
  readonly recognizable?: unknown;
  readonly needsRefinement?: unknown;
}): ThreadTitleVerdict {
  const values = [result.grounded, result.specific, result.recognizable];
  if (result.needsRefinement === true || values.includes(false)) return "insufficient";
  return values.every((value) => value === true) ? "sufficient" : "unknown";
}

export const assessmentAddendum = `

Automatic title assessment (supersedes earlier needsRefinement instructions, including the regeneration instruction to set it to false):
In this SAME response return title, needsRefinement, grounded, specific, recognizable.
All three quality fields must be booleans. Set needsRefinement to the negation of their conjunction.
- grounded: the title describes a concrete goal adopted by the user in the supplied evidence. An explicitly adopted handoff Goal is valid even when historical; an unclear reference to another conversation alone is weak evidence.
- specific: it names the actual subject and desired outcome, rather than a generic process, model/mode, identifier, diagnostic symptom, or handoff mechanics.
- recognizable: the user could distinguish this thread from similar threads weeks later from this title.
Capture the umbrella installation/update goal when setup, update choices, storage and backup steps serve that goal.
Use only the supplied bounded evidence. Do not investigate ancestors, linked conversations, issues, PRs, or local git history. Do not use tools or wait for an answer.
Assess the title you return, not the previous title. Do not invent a goal to obtain a passing assessment.`;
