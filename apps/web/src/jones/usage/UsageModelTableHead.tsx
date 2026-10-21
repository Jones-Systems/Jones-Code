export function UsageModelTableHead() {
  return (
    <thead>
      <tr className="border-b border-border text-right text-xs text-muted-foreground whitespace-nowrap">
        <th className="py-2 pr-3 text-left font-normal">#</th>
        <th className="w-full py-2 text-left font-normal">Model</th>
        <th className="py-2 pl-3 font-normal @2xl:pl-6">Cost</th>
        <th aria-label="Cost percentage" className="hidden py-2 pl-6 font-normal @2xl:table-cell">
          %
        </th>
        <th className="py-2 pl-3 font-normal @2xl:pl-6">Tokens</th>
        <th aria-label="Token percentage" className="hidden py-2 pl-6 font-normal @2xl:table-cell">
          %
        </th>
      </tr>
    </thead>
  );
}
