# Deterministic ordering candidate

Opt-in `ordered_cdc` adds a string `_planetscale_order` column and discovers it as the source-defined destination cursor. Existing behavior is unchanged when disabled. The column must be selected as the destination cursor for deduplication; extraction remains VStream based.

The version is a fixed-width tuple of MySQL56 GTID set cardinality, snapshot phase, and row ordinal within the committed transaction. A snapshot sorts after CDC at the same position. Snapshot rows use a constant ordinal so resuming at a different primary key or changing packet boundaries does not change their version. CDC row ordinals span response packets and distinguish repeated updates to one primary key within a transaction. Source timestamps are not used for ordering.

Rows and pending positions are held until commit. An interrupted transaction emits no buffered rows and retains the previous committed checkpoint. Memory is capped at 64 MiB of serialized row events per transaction; oversized transactions fail rather than skip data. Source fields are retained with each buffered row. Existing optional metadata remains available.

This candidate is restricted to the unsharded `-` shard. Successive positions must contain the previous GTID set. A new primary UUID is supported when its history extends the previous set; divergent or regressing histories and resharding fail. Deleted rows remain subject to the connector's existing behavior; this change does not implement deletion propagation.

## Validation

Tests cover repeated updates, exact replay, several transactions in one response, transactions split across responses, interrupted commits, rollback, snapshot resume, CDC during copy, primary UUID changes, incomparable histories, and memory limits. Existing source tests also pass under Go 1.22.12.

Before production use, run an isolated Airbyte trial with a separate destination prefix. Verify discovered schema, generated BigQuery merge ordering, resumed VStream state, snapshot completion, and source/destination row comparisons. Production cursors and historical tables have not been changed for this candidate.

When deploying, retain the current GTID checkpoint, add the column through schema discovery, and select it as the destination cursor. Existing rows initially have a null ordering value; the first newer source event should supersede them. A historical repair requires a separately validated retain-records refresh. Rolling back the image also requires restoring the old configured cursor; never rewind the VStream state.
