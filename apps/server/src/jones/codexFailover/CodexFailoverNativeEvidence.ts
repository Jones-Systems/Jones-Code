import type { ServerNotification__CodexErrorInfo } from "effect-codex-app-server/schema";

type ErrorInfo = ServerNotification__CodexErrorInfo | null | undefined;
interface NativeFailure {
  readonly message?: string | undefined;
  readonly codexErrorInfo?: ErrorInfo;
}

export function codexFailoverContraryInfo(info: ErrorInfo): boolean {
  if (info === "badRequest" || info === "unauthorized") return true;
  if (info === null || typeof info !== "object") return false;
  const status =
    "httpConnectionFailed" in info
      ? info.httpConnectionFailed.httpStatusCode
      : "responseStreamConnectionFailed" in info
        ? info.responseStreamConnectionFailed.httpStatusCode
        : "responseStreamDisconnected" in info
          ? info.responseStreamDisconnected.httpStatusCode
          : "responseTooManyFailedAttempts" in info
            ? info.responseTooManyFailedAttempts.httpStatusCode
            : null;
  return status === 400 || status === 401 || status === 403;
}

// A session owns this bounded memory; owner identity fences native ID reuse.
export function makeCodexFailoverNativeEvidence<Owner>() {
  const failures = new Map<string, { readonly owner: Owner; readonly error: NativeFailure }>();
  return {
    observe(nativeTurnId: string, owner: Owner, error: NativeFailure) {
      failures.delete(nativeTurnId);
      failures.set(nativeTurnId, { owner, error: { ...error } });
      if (failures.size > 1024) failures.delete(failures.keys().next().value!);
    },
    take(nativeTurnId: string, owner: Owner, completion: NativeFailure): ErrorInfo {
      const notification = failures.get(nativeTurnId);
      failures.delete(nativeTurnId);
      if (completion.codexErrorInfo != null) return completion.codexErrorInfo;
      return notification?.owner === owner &&
        (completion.message === undefined || completion.message === notification.error.message)
        ? notification.error.codexErrorInfo
        : undefined;
    },
    forget(nativeTurnId: string) {
      failures.delete(nativeTurnId);
    },
    clear() {
      failures.clear();
    },
  };
}
