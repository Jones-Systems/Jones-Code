import { AuthSessionId } from "@t3tools/contracts";
import { enrolledContext } from "../nativeProvider/testFixtures.ts";
import type { NativeEnrollmentRequest } from "./request.ts";

export const enrollmentRequest: NativeEnrollmentRequest = {
  schema: "jones-code.workstreams-native-enrollment/v1",
  enrollment_id: enrolledContext.enrollment_id,
  registry_origin: "https://registry.invalid",
  context: enrolledContext,
  session: {
    session_id: AuthSessionId.make("enrollment-session-synthetic"),
    issued_at: "2026-01-01T00:00:00.000Z",
    expires_at: "2026-01-31T00:00:00.000Z",
  },
};
