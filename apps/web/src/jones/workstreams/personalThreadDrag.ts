export function liftPersonalThreadCard(source: HTMLElement, origin: { x: number; y: number }) {
  const rect = source.getBoundingClientRect();
  const view = document.createElement("div");
  view.dataset.personalThreadDrag = "";
  view.setAttribute("aria-hidden", "true");
  view.inert = true;
  // Match the pinned row's lifted surface while retaining the rendered title card.
  view.className =
    "pointer-events-none fixed left-0 top-0 z-[100] rounded-md bg-sidebar bg-linear-to-b from-sidebar-row-active to-sidebar-row-active text-sidebar-foreground shadow-lg";
  view.style.width = `${rect.width}px`;
  const card = source.cloneNode(true) as HTMLElement;
  for (const node of [card, ...card.querySelectorAll<HTMLElement>("*")]) {
    node.removeAttribute("id");
    node.removeAttribute("data-personal-thread");
    node.style.setProperty("pointer-events", "none", "important");
  }
  view.append(card);
  document.body.append(view);
  const opacity = source.style.opacity;
  source.style.opacity = "0.3";
  source.dataset.personalThreadDragSource = "";
  const move = (point: { x: number; y: number }) => {
    view.style.transform = `translate3d(${rect.left}px, ${rect.top + point.y - origin.y}px, 0)`;
  };
  move(origin);
  return {
    move,
    dispose: () => {
      view.remove();
      source.style.opacity = opacity;
      delete source.dataset.personalThreadDragSource;
    },
  };
}
