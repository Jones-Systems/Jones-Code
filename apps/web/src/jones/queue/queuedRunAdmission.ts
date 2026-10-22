export function canDispatchQueuedRun(input: {
  importedHistory: boolean;
  busy: boolean;
  canStart: boolean;
  canSteer: boolean;
  queuedRunStart: boolean | undefined;
}): boolean {
  return (
    !input.importedHistory &&
    !input.busy &&
    (input.canStart ? input.queuedRunStart === true : input.canSteer)
  );
}
