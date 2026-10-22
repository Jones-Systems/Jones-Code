export const JONES_SHELL_LANGUAGE = "jones-shell";

export type ShellDestinationFence =
  | { _tag: "Valid"; environmentId: string; label?: string; title?: string }
  | { _tag: "Invalid"; reason: string };

/** Parses metadata only; labels never resolve targets and command eligibility belongs to the client. */
export function parseShellDestinationFence(meta: string | null | undefined): ShellDestinationFence {
  if (!meta || meta.trim().length === 0) {
    return { _tag: "Invalid", reason: "An exact environment ID is required." };
  }
  if (meta.length > 4096) {
    return { _tag: "Invalid", reason: "Shell destination metadata is too long." };
  }
  if (/[\p{Cc}\p{Cf}\p{Zl}\p{Zp}]/u.test(meta)) {
    return { _tag: "Invalid", reason: "Shell destination metadata must not contain controls." };
  }

  // Spaces separate options. Quoted values escape only their quote or a backslash.
  const option = /([a-z]+)=("(?:[^"\\]|\\["\\])*"|'(?:[^'\\]|\\['\\])*'|[^\s"'\\=]+)/y;
  const values: { environment?: string; label?: string; title?: string } = {};
  let cursor = 0;
  while (cursor < meta.length) {
    if (meta[cursor] === " ") {
      cursor += 1;
      continue;
    }
    option.lastIndex = cursor;
    const match = option.exec(meta);
    if (!match || (option.lastIndex < meta.length && meta[option.lastIndex] !== " ")) {
      return {
        _tag: "Invalid",
        reason: "Use space-separated key=value options with properly quoted values.",
      };
    }
    const [, key, raw] = match;
    if (key !== "environment" && key !== "label" && key !== "title") {
      return {
        _tag: "Invalid",
        reason: "Only environment, label, and title options are supported.",
      };
    }
    if (values[key] !== undefined) {
      return { _tag: "Invalid", reason: "Each shell destination option must appear only once." };
    }
    if (raw === undefined) {
      return { _tag: "Invalid", reason: "Each shell destination option requires a value." };
    }
    const value =
      raw.startsWith('"') || raw.startsWith("'")
        ? raw.slice(1, -1).replace(/\\(["'\\])/g, "$1")
        : raw;
    if (value.trim().length === 0) {
      return { _tag: "Invalid", reason: "Shell destination option values must not be blank." };
    }
    values[key] = value;
    cursor = option.lastIndex;
  }
  const environmentId = values.environment;
  if (environmentId === undefined) {
    return { _tag: "Invalid", reason: "An exact environment ID is required." };
  }
  // EnvironmentId is a trimmed nonempty string, not a UUID or a machine-name prefix.
  // Reject surrounding whitespace instead of silently changing the requested destination.
  if (environmentId !== environmentId.trim() || environmentId.length > 1024) {
    return {
      _tag: "Invalid",
      reason: "The environment ID must be exact and at most 1024 characters.",
    };
  }
  return {
    _tag: "Valid",
    environmentId,
    ...(values.label !== undefined ? { label: values.label } : {}),
    ...(values.title !== undefined ? { title: values.title } : {}),
  };
}
