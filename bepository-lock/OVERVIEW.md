# bepository-lock

Distributed exclusive leases using `object_store` `put_if_not_exists` and object
timestamps. Does not rely on system clocks to be synchronized.

## Data Model

- **Epoch Files**: `00000000.json` (8-char Crockford Base32). Numeric order =
  filename order.
- **Content**: JSON with `holder` (ID), `priority` (int), and `duration`
  (seconds).
- **ID Recommendation**: Use `machine_id/resource` to allow immediate scavenging
  of previous session's files.
- **Timing**: Decisions use store `Last-Modified` timestamps. `Now` is observed
  from newer files.

## Ownership Rules

1. **Owner**: Holder of the **lowest-named** non-expired file.
2. **Expiry**: File `E` is expired if `E.timestamp + E.duration <= Now`.
3. **Preemption**: Higher priority participants "queue" by leaving their epoch
   file to block lower-priority renewals.

## Lock Acquisition Algorithm

1. **Claim**: `LIST` highest epoch, `put_if_not_exists` for `highest + 1`.
   Record creation timestamp.
2. **Scavenge**: Delete lower files if **Expired** (`E.ts + E.dur <= your.ts`)
   or **Own File** (leftover from previous session). Skip if `E.ts > your.ts`
   (store clock regression).
3. **Verify**: `LIST` the directory again:
   - **If Lowest**: Check for yield. If any higher file `C` has
     `C.priority > your.priority` and is not expired relative to
     `your.timestamp`, delete self and return `Yielded`. Otherwise, return
     `Owner`.
   - **If Not Lowest**:
     - If any lower file `E` from a different holder has `E.ts > your.ts`,
       return `QueuedBackwardClock`.
     - If `your.priority > owner.priority`, return `Queued`.
     - Otherwise, delete self and return `Failed`.

## Renewal & Maintenance

- **Renewal**: Re-run the full Acquisition algorithm. Your new file replaces the
  old one via the "Own File" scavenge rule.
- **Strategy**: Sleep `lease/3`. On I/O error, retry every 15s. Give up
  (`Expired`) at `2*lease/3` elapsed since last success.
- **Release**: Callers MUST explicitly call `release()` to delete the epoch
  file.

## Critical Rules & Gotchas

- **No Clone**: `LockGuard` is not `Clone`. Cloning would create racing renewal
  tasks.
- **Drop Fallback**: `Drop` cancels the renewal task but **cannot** perform
  async I/O to delete the file. Epoch files linger until scavenged if
  `release()` is skipped.
- **Clock Safety**: Immune to local clock drift; requires store clock
  monotonicity for automated resolution of regressions.
- **Mutual Exclusion**: Best-effort. Overlap may occur during store clock jumps
  or process freezes.
