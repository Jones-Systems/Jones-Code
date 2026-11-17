interface WorkspacePackage {
  readonly name: string;
  readonly dir: string;
  readonly hasTest: boolean;
  readonly deps: ReadonlyArray<string>;
}

export const TEST_FILE: RegExp;
export function splitTests(tests: Iterable<string>): {
  vitestTests: Array<string>;
  nodeTests: Array<string>;
};

export function requiresFullQualification(files: ReadonlyArray<string> | null): boolean;

export function ciNotes(
  files: ReadonlyArray<string> | null,
  packages: ReadonlyArray<WorkspacePackage>,
): Array<string>;
