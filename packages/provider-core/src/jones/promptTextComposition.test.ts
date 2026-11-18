import { assert, it } from "@effect/vitest";
import { ProviderInstanceId, TextGenerationError } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import { decodeJsonReply, fromRunner } from "../server/textGenerationOperations.ts";
import { buildThreadTitlePrompt } from "../server/textGenerationPrompts.ts";

const modelSelection = { instanceId: ProviderInstanceId.make("synthetic"), model: "synthetic" };

it.effect(
  "composes assessed titles with project, fork, linked and custom context in one call",
  () =>
    Effect.gen(function* () {
      const input = {
        cwd: "/synthetic",
        message: "Choose installation updates",
        projectTitle: "Example project",
        previousTitle: "Old installation title",
        forkSource: { title: "Installation options", history: "Adopt the maintenance goal." },
        linkedContext: "The owner adopted this goal in the supplied handoff.",
        instructions: "Use a concrete maintenance title.",
        attachments: [],
        titleAssessment: true,
        modelSelection,
      };
      let calls = 0;
      const service = fromRunner("Synthetic", (request) => {
        calls++;
        assert.equal(request.operation, "generateThreadTitle");
        assert.equal(request.cwd, input.cwd);
        assert.strictEqual(request.modelSelection, modelSelection);
        assert.strictEqual(request.attachments, input.attachments);
        assert.equal(request.prompt, buildThreadTitlePrompt(input).prompt);
        assert.include(request.prompt, input.projectTitle);
        assert.include(request.prompt, input.forkSource.history);
        assert.include(request.prompt, input.linkedContext);
        assert.include(request.prompt, input.instructions);
        assert.include(request.prompt, "Automatic title assessment");
        return decodeJsonReply(
          request,
          "Synthetic",
          '{"title":"Installation updates","needsRefinement":true,"grounded":true,"specific":false,"recognizable":true}',
        );
      });

      assert.deepEqual(yield* service.generateThreadTitle(input), {
        title: "Installation updates",
        needsRefinement: true,
        assessment: "insufficient",
      });
      assert.equal(calls, 1);
    }),
);

it.effect("keeps unassessed custom and legacy title results without a synthetic verdict", () =>
  Effect.gen(function* () {
    for (const titleAssessment of [undefined, false]) {
      const input = {
        cwd: "/synthetic",
        message: "Fix the parser",
        instructions: "Use sentence case.",
        titleAssessment,
        modelSelection,
      };
      const service = fromRunner("Synthetic", (request) => {
        assert.equal(request.prompt, buildThreadTitlePrompt(input).prompt);
        assert.notInclude(request.prompt, "Automatic title assessment");
        return decodeJsonReply(
          request,
          "Synthetic",
          '{"title":"Fix parser","needsRefinement":true,"grounded":false}',
        );
      });
      assert.deepEqual(yield* service.generateThreadTitle(input), {
        title: "Fix parser",
        needsRefinement: true,
      });
    }
  }),
);

it.effect("rejects placeholder titles when automatic assessment is requested", () =>
  Effect.gen(function* () {
    const service = fromRunner("Synthetic", (request) =>
      decodeJsonReply(
        request,
        "Synthetic",
        '{"title":"title","grounded":true,"specific":true,"recognizable":true}',
      ),
    );
    const error = yield* service
      .generateThreadTitle({
        cwd: "/synthetic",
        message: "Fix the parser",
        titleAssessment: true,
        modelSelection,
      })
      .pipe(Effect.flip);
    assert.instanceOf(error, TextGenerationError);
    assert.equal(error.operation, "generateThreadTitle");
  }),
);
