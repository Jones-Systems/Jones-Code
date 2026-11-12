import * as Schema from "effect/Schema";
import * as HttpApiEndpoint from "effect/http-api/HttpApiEndpoint";
import * as HttpApiGroup from "effect/http-api/HttpApiGroup";
import * as HttpApiSchema from "effect/http-api/HttpApiSchema";
import type { makeJonesHttpGroups } from "./environmentHttpGroups.ts";
import { ThreadId } from "../baseSchemas.ts";
import {
  PREVIEW_COMPANION_HTTP_BASE,
  PreviewCompanionHostsResponse,
  PreviewCompanionDefaultSelectionInput,
  PreviewCompanionThreadSelectionInput,
  PreviewCompanionThreadSelectionResponse,
} from "./previewCompanion.ts";

export function makePreviewCompanionHttpGroup({
  OptionalBearerHeaders,
  EnvironmentAuthenticatedAuth,
  EnvironmentScopeRequiredError,
  EnvironmentInternalError,
  EnvironmentHttpBadRequestError,
}: Parameters<typeof makeJonesHttpGroups>[0]) {
  const params = Schema.Struct({
    threadId: ThreadId.check(Schema.isMaxLength(128), Schema.isPattern(/^[^/]+$/)),
  });
  const common = {
    headers: OptionalBearerHeaders,
    error: [
      EnvironmentScopeRequiredError,
      EnvironmentInternalError,
      EnvironmentHttpBadRequestError,
    ],
  } as const;
  return HttpApiGroup.make("previewCompanion")
    .add(
      HttpApiEndpoint.get("hosts", `${PREVIEW_COMPANION_HTTP_BASE}/hosts`, {
        ...common,
        success: PreviewCompanionHostsResponse,
      }).middleware(EnvironmentAuthenticatedAuth),
    )
    .add(
      HttpApiEndpoint.put("setDefault", `${PREVIEW_COMPANION_HTTP_BASE}/default`, {
        ...common,
        payload: PreviewCompanionDefaultSelectionInput,
        success: Schema.Void.pipe(HttpApiSchema.status(204)),
      }).middleware(EnvironmentAuthenticatedAuth),
    )
    .add(
      HttpApiEndpoint.get("thread", `${PREVIEW_COMPANION_HTTP_BASE}/threads/:threadId`, {
        ...common,
        params,
        success: PreviewCompanionThreadSelectionResponse,
      }).middleware(EnvironmentAuthenticatedAuth),
    )
    .add(
      HttpApiEndpoint.put("setThread", `${PREVIEW_COMPANION_HTTP_BASE}/threads/:threadId`, {
        ...common,
        params,
        payload: PreviewCompanionThreadSelectionInput,
        success: Schema.Void.pipe(HttpApiSchema.status(204)),
      }).middleware(EnvironmentAuthenticatedAuth),
    );
}
