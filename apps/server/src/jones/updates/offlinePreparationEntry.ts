export function offlinePreparationStatus() {
  return {
    status: "held",
    reason: "maintenance-owner-binding-unqualified",
    databaseOpened: false,
    productionInvocation: false,
    activation: false,
  } as const;
}

/** Standalone invocation reports the held state without opening application state. */
export function offlinePreparationEntryOutput(): string {
  // This fixed native entry serializes only its closed held-status literal, without loading the application or schema runtime.
  return `${JSON.stringify(offlinePreparationStatus())}\n`;
}

/** The only executable dispatch reads the real inherited-process invocation. */
export async function offlinePreparationMain(): Promise<void> {
  if (process.argv.length === 2) {
    process.stdout.write(offlinePreparationEntryOutput());
    return;
  }
  const worker = await import("./offlinePreparationWorker.ts");
  if (worker.preparationWorkerArguments(process.argv.slice(2)) === null) {
    process.exitCode = 64;
    process.stdout.write(offlinePreparationEntryOutput());
    return;
  }
  try {
    await worker.runInheritedPreparationWorker();
  } catch {
    process.exitCode = 70;
    process.stderr.write("Closed preparation worker refused.\n");
  }
}

// SEA module entries expose import.meta.main without filesystem entry detection.
if (import.meta.main) await offlinePreparationMain();
