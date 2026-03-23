# bepository-e2etest

End-to-end tests that exercise `bepository` as a real subprocess against live Go
masters. Validates the full pipeline: ingest from multiple masters into shared
cold storage, then retrieval by a fresh master.

- **Subprocess Execution**: We intentionally spawn `bepository` as a separate
  subprocess to exercise the actual compiled CLI binary, environment variable
  loading, CLI flag parsing, and realistic process lifecycle (startup/shutdown).
- **Go Masters**: Tests orchestrate live, throwaway Go Syncthing instances to
  exercise compat.
- **Backend**: Uses throwaway local paths simulating real object storage
  behavior.
