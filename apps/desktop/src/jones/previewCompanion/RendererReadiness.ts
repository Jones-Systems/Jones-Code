export function watchCompanionRenderer(
  renderer: NodeJS.EventEmitter,
  onUnavailable: () => void,
): () => void {
  const unwatch = () => {
    renderer.removeListener("destroyed", stop);
    renderer.removeListener("render-process-gone", stop);
    renderer.removeListener("did-start-navigation", navigate);
  };
  const stop = () => {
    unwatch();
    onUnavailable();
  };
  const navigate = (_event: unknown, _url: string, inPlace: boolean, mainFrame: boolean) => {
    if (mainFrame && !inPlace) stop();
  };
  renderer.once("destroyed", stop);
  renderer.once("render-process-gone", stop);
  renderer.on("did-start-navigation", navigate);
  return unwatch;
}
