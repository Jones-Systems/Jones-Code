# Frozen Workstream v1 conformance fixtures

These files are byte-for-byte copies of the accepted control-plane fixture corpus at source HEAD
`83d4a342672abd7ba0a3eb440b63e75fe0e5789e`:
`contracts/workstreams/v1/fixtures/` in the `chatgpt-workstream-core-v1` worktree.

- `conformance.json.fixture`: `a907dfa46f18a1930d0bd176571743d4932ae6dd13fc96421d5dec8e63bd6aa1`
- `negative-cases.json.fixture`: `d4893333b797c53249f37cd9cc04c605ca54eeed542dcab0c62f6a6c91093d50`
- `semantic-cases.json.fixture`: `1d939c25a2f9b9aca0191b390ba8739fcd8f6d2e40bb0f1e6ae389c2eb05a6fa`

The accepted historical specification binding is
`c42242c8e33aa4c83d08c82c5c1b8bb2099bbfcb`. Tests execute T3's supported DTO decoders against
the relevant positive and negative cases; this directory does not claim that T3 reimplements the
entire upstream registry schema.

The `.fixture` suffix prevents repository formatters from rewriting the immutable JSON bytes.
