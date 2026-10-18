interface ProfilePath {
  readonly isAbsolute: (value: string) => boolean;
  readonly normalize: (value: string) => string;
}

export function resolveDesktopUserDataOverride(
  value: string | undefined,
  path: ProfilePath,
): string | null {
  const directory = value?.trim();
  if (!directory) return null;
  if (directory.includes("\0")) {
    throw new Error("T3CODE_DESKTOP_USER_DATA_DIR must not contain a null byte.");
  }
  if (!path.isAbsolute(directory)) {
    throw new Error("T3CODE_DESKTOP_USER_DATA_DIR must be an absolute path.");
  }
  return path.normalize(directory);
}

export function configureDesktopUserDataOverride(input: {
  readonly directory: string | undefined;
  readonly path: ProfilePath;
  readonly createDirectory: (directory: string) => void;
  readonly setPath: (name: "userData" | "sessionData", directory: string) => void;
}): void {
  const directory = resolveDesktopUserDataOverride(input.directory, input.path);
  if (directory === null) return;
  input.createDirectory(directory);
  input.setPath("userData", directory);
  input.setPath("sessionData", directory);
}
