import * as Schema from "effect/Schema";
import { ClientSurface, TrimmedNonEmptyString } from "../baseSchemas.ts";

// Presentation only; these hints never participate in authentication or scope checks.
export const jonesAuthClientPresentationFields = {
  surface: Schema.optionalKey(ClientSurface),
  appVersion: Schema.optionalKey(TrimmedNonEmptyString.check(Schema.isMaxLength(64))),
};
export const JonesAuthClientPresentation = Schema.Struct(jonesAuthClientPresentationFields);
export type JonesAuthClientPresentation = typeof JonesAuthClientPresentation.Type;
