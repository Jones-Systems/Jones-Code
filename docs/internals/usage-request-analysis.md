# Request-level usage analysis boundary

This document proposes a future request-analysis seam. It does not add runtime behavior or change the current Usage page. The standalone token-info tool and its personal report website are the first consumer; Jones Code integration remains a draft.

The current transcript reducers and incremental scan cache answer spending questions by time, provider, model and source. Cache diagnostics need a different unit: individual model requests within a user turn, with the preceding turn retained as evidence. Summing a turn can hide a cold first request followed by warm tool continuations. A low cached-input ratio is an observation, not proof that a cache expired.

## Preserve identity before aggregation

A portable record should retain a provider request identity where available, native session and turn identities, observed usage time and its timing basis, numeric counters and their availability, and source byte/record provenance. Do not identify requests by equal token vectors: distinct requests may have identical counts, and copied transcripts may repeat the same provider request. Conflicting observations and ID-less legacy snapshots need explicit partitions rather than being added to verified totals. Claude streaming message blocks require provider-specific reconciliation rather than one request per block.

Cached reads and cache writes are subsets of Codex total input. Claude ordinary input, reads and writes must be combined to obtain total input. Reasoning tokens are already included in output. Missing counters remain unknown. Modeled API prices, purchased credits and observed account charges are different accounting units.

## Account and boundary evidence

Provider instance IDs identify configured routing slots. They are not authenticated-account IDs. Apply an explicitly attested account mapping to the instance that served the request, using historical session/model-selection events. Current runtime bindings overwrite previous bindings, so they cannot attribute an entire history after an instance switch. Codex authentication overlays share transcript directories; source-directory names alone do not establish the serving instance. A variable-account default instance stays separate unless contemporaneous account evidence exists.

Retain turn start/completion and compaction boundaries before reducing usage. Native turn IDs can join request observations to T3 turn records; compaction activities often identify a turn without identifying an API request. Model, effort, settings, session and instance changes remain separate confounders. An idle gap derived from usage-report timestamps must be labeled as a proxy when true request-start/completion times are unavailable.

## Incremental and portable consumers

Keep provider parsing ordered within each file, with resumable context and cumulative baselines. A cursor becomes durable in the same transaction as its observations. Read only a captured file prefix and complete newline records; unfinished tails remain for the next scan. File replacement, truncation and guard failure invalidate that source generation. An append-only fast path is an explicit validation assumption, not proof that arbitrary historical bytes never changed. Independent files can be parsed by bounded workers while one index writer commits results.

Use a versioned metadata-only JSON seam for external reports rather than exposing server settings, Effect services or raw transcript payloads. A report binds its query window, captured source snapshot, mapping/rate/detector revisions, coverage and unknowns. Filters must conserve totals. Resolve the chronological predecessor before applying display filters, even when the predecessor is outside the selected window.

Candidate detection should initially favor recall. Immutable packets retain the entire suspected turn and the preceding turn, including every captured request, boundary evidence and selection membership. Separate annotations record true-positive, false-positive or unknown labels against packet hashes. Labels do not rewrite measurements or turn an idle association into a cache-lifetime guarantee.

The standalone implementation owns ingestion and report generation; its static website browses saved reports. A future Jones Code consumer can reuse that data boundary without adding periodic log scans or sending raw histories over the WebSocket. Existing reducers, guarded readers and scan-cache code are reusable references, while request identity, missingness and boundary evidence require richer contracts.
