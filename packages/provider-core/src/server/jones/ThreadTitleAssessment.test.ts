import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { ProviderInstanceId } from "@t3tools/contracts";
import { buildThreadTitlePrompt } from "../textGenerationPrompts.ts";
import { fromRunner, decodeJsonReply } from "../textGenerationOperations.ts";
import { threadTitleVerdict } from "./ThreadTitleAssessment.ts";

const decodeAssessment = Schema.decodeSync(
  buildThreadTitlePrompt({ message: "", titleAssessment: true }).outputSchema,
);

describe("title assessment", () => {
  it("uses one explicit three-boolean verdict and preserves legacy insufficiency", () => {
    const pass = { grounded: true, specific: true, recognizable: true };
    assert.equal(threadTitleVerdict(pass), "sufficient");
    assert.equal(threadTitleVerdict({ ...pass, needsRefinement: true }), "insufficient");
    for (const field of ["grounded", "specific", "recognizable"]) {
      assert.equal(threadTitleVerdict({ ...pass, [field]: false }), "insufficient");
      assert.equal(threadTitleVerdict({ ...pass, [field]: "true" }), "unknown");
      assert.equal(threadTitleVerdict({ ...pass, [field]: undefined }), "unknown");
    }
    assert.equal(threadTitleVerdict({ needsRefinement: false }), "unknown");
    assert.equal(
      threadTitleVerdict({ grounded: false, needsRefinement: "invalid" }),
      "insufficient",
    );
  });

  it("leaves manual prompts and schema byte-identical and supersedes regeneration's legacy rule", () => {
    for (const previousTitle of [undefined, "Old title"]) {
      const input = { message: "Goal: choose installation and updates", previousTitle };
      const legacy = buildThreadTitlePrompt(input);
      const manual = buildThreadTitlePrompt({ ...input, titleAssessment: false });
      assert.equal(manual.prompt, legacy.prompt);
      assert.deepEqual(
        Schema.toJsonSchemaDocument(manual.outputSchema),
        Schema.toJsonSchemaDocument(legacy.outputSchema),
      );
      const assessed = buildThreadTitlePrompt({ ...input, titleAssessment: true });
      assert.isTrue(assessed.prompt.startsWith(legacy.prompt));
      assert.include(assessed.prompt, "supersedes earlier needsRefinement instructions");
      assert.equal(threadTitleVerdict(decodeAssessment({ title: "Missing fields" })), "unknown");
      assert.equal(
        threadTitleVerdict(
          decodeAssessment({
            title: "Invalid",
            grounded: "yes",
            recognizable: true,
          }),
        ),
        "unknown",
      );
    }
  });

  it.effect("common provider runner returns assessment in the same call", () =>
    Effect.gen(function* () {
      let count = 0;
      const service = fromRunner("test", (request) => {
        count++;
        return decodeJsonReply(
          request,
          "test",
          '{"title":"Installation choices","needsRefinement":false,"grounded":true,"specific":true,"recognizable":true}',
        );
      });
      const result = yield* service.generateThreadTitle({
        cwd: "/synthetic",
        message: "install",
        titleAssessment: true,
        modelSelection: { instanceId: ProviderInstanceId.make("test"), model: "test" },
      });
      assert.equal(result.assessment, "sufficient");
      assert.equal(count, 1);
    }),
  );
});
