import { PullRequestOperationError } from "@t3tools/contracts";
import { useState } from "react";
import { createRoot } from "react-dom/client";
import "../../../index.css";
import { Button } from "../../../components/ui/button";
import { shouldRefreshOnArrival } from "../../../hooks/useLiveRefresh";
import { PullRequestReadPauseBanner } from "../PullRequestReadPauseBanner";

const originalObservation = Date.parse("2026-09-01T12:00:00Z");
const retryAt = Date.parse("2026-09-01T13:00:00Z");
const paused = new PullRequestOperationError({
  operation: "detail",
  detail: "Provider reads are paused.",
  reason: "rate-limited",
  retryAt,
});

function Fixture() {
  const [failure, setFailure] = useState<PullRequestOperationError | null>(null);
  const [now, setNow] = useState(0);
  const [lastRead, setLastRead] = useState(0);
  const [reads, setReads] = useState(0);
  const [visible, setVisible] = useState(true);
  const [observedAt, setObservedAt] = useState<number | undefined>(originalObservation);
  const [title, setTitle] = useState("Keep pull request details readable during a pause");
  const [editing, setEditing] = useState(false);
  const [events, setEvents] = useState<string[]>([]);

  function arrive(seconds: number) {
    const next = seconds * 1_000;
    setNow(next);
    const allowed = shouldRefreshOnArrival({
      visible,
      now: next,
      lastRefreshedAt: lastRead,
      minimumIntervalMs: 60_000,
    });
    if (allowed) {
      setLastRead(next);
      setReads((count) => count + 1);
    }
    setEvents((previous) => [...previous, `${seconds}s arrival: ${allowed ? "read" : "held"}`]);
  }

  return (
    <main className="mx-auto max-w-3xl space-y-6 p-8 text-foreground">
      <header className="space-y-2">
        <h1 className="text-2xl font-semibold">Pull request read fixture</h1>
        <p className="text-sm text-muted-foreground">
          Synthetic controls exercise the production banner and arrival predicate. Reads and
          invalidation below are simulated; no provider is contacted.
        </p>
      </header>
      <section className="space-y-3 rounded-lg border p-4">
        <div className="flex flex-wrap gap-2">
          {[10, 59, 60].map((seconds) => (
            <Button key={seconds} variant="outline" onClick={() => arrive(seconds)}>
              Arrive at {seconds}s
            </Button>
          ))}
          <Button variant="outline" onClick={() => setVisible((value) => !value)}>
            {visible ? "Hide view" : "Show view"}
          </Button>
          <Button
            variant="outline"
            onClick={() => {
              setNow(0);
              setLastRead(0);
              setReads(0);
              setEvents([]);
            }}
          >
            Reset clock
          </Button>
        </div>
        <p data-testid="read-count">Simulated reads: {reads}</p>
        <p>
          Clock: {now / 1_000}s · View: {visible ? "visible" : "hidden"}
        </p>
      </section>
      <section className="overflow-hidden rounded-lg border">
        <PullRequestReadPauseBanner
          detailFailure={failure}
          activityFailure={null}
          hasDetail={true}
          observedAt={observedAt}
        />
        <div className="space-y-4 p-4">
          <p className="text-xs text-muted-foreground">Jones-Systems/example #42 · Open</p>
          {editing ? (
            <label className="flex flex-col gap-2">
              Pull request title
              <input
                aria-label="Pull request title"
                className="rounded border p-2"
                value={title}
                onChange={(event) => setTitle(event.target.value)}
              />
            </label>
          ) : (
            <h2 className="text-xl font-semibold">{title}</h2>
          )}
          <div className="flex flex-wrap gap-2">
            <Button variant="outline" onClick={() => setFailure(paused)}>
              Pause reads
            </Button>
            <Button variant="outline" onClick={() => setFailure(null)}>
              Recover
            </Button>
            <Button variant="outline" onClick={() => setObservedAt(undefined)}>
              Unknown observation
            </Button>
            <Button variant="outline" onClick={() => setEditing((value) => !value)}>
              {editing ? "Save title" : "Edit title"}
            </Button>
            <Button
              onClick={() => {
                setEvents((previous) => [...previous, "manual invalidate", "manual refresh"]);
                setReads((count) => count + 1);
              }}
            >
              Refresh manually
            </Button>
          </div>
        </div>
      </section>
      <section className="space-y-2">
        <h2 className="font-semibold">Simulated request log</h2>
        <ol>
          {events.map((event, index) => (
            <li key={index}>{event}</li>
          ))}
        </ol>
      </section>
    </main>
  );
}

createRoot(document.getElementById("root")!).render(<Fixture />);
