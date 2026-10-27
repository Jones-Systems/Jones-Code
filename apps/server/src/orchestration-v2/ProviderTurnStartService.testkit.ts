import * as RestartPreparation from "../jones/orchestration/RestartPreparation.ts";
import * as ControlAdmission from "../jones/orchestration/ControlAdmission.ts";
import * as ContinuityAdmissionTestkit from "../jones/goals/continuity/admission.testkit.ts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import * as GitWorkflow from "../git/GitWorkflowService.ts";
import * as ProjectService from "../project/ProjectService.ts";

export const layer = Layer.mergeAll(
  ContinuityAdmissionTestkit.layerAbsent,
  ControlAdmission.layer,
  Layer.mock(RestartPreparation.RestartPreparation)({ assertThreadOpen: () => Effect.void }),
  Layer.mock(GitWorkflow.GitWorkflowService)({
    pruneWorktrees: () => Effect.void,
    createWorktree: () => Effect.succeed({} as never),
  }),
  Layer.mock(ProjectService.ProjectService)({
    getById: () => Effect.succeedNone,
  }),
);
