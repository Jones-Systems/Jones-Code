export function listenForWorkspaceFileDragEnd(clearDrag: () => void) {
  // Nested reply/composer drops stop bubbling; OS file drags may never emit dragend.
  window.addEventListener("drop", clearDrag, true);
  window.addEventListener("dragend", clearDrag);
  return () => {
    window.removeEventListener("drop", clearDrag, true);
    window.removeEventListener("dragend", clearDrag);
  };
}
