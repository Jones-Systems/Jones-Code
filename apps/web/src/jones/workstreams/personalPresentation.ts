export function moveNativeThreadOrder(
  order: readonly string[],
  movedId: string,
  neighborId: string,
  after: boolean,
): readonly string[] {
  if (movedId === neighborId || !order.includes(movedId) || !order.includes(neighborId))
    return order;
  const next = order.filter((key) => key !== movedId);
  next.splice(next.indexOf(neighborId) + (after ? 1 : 0), 0, movedId);
  return next;
}
