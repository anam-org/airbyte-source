# Deterministic ordering candidate

Opt-in `ordered_cdc` adds a string `_planetscale_order` column and discovers it as the source-defined destination cursor. Existing behavior is unchanged when disabled. The column must be selected as the destination cursor for deduplication; extraction remains VStream based.

The version is a fixed-width tuple of MySQL56 GTID set cardinality, snapshot phase, and row ordinal within the committed transaction. A snapshot sorts after CDC at the same position. Snapshot rows use a constant ordinal so resuming at a different primary key or changing packet boundaries does not change their version. CDC row ordinals span response packets and distinguish repeated updates to one primary key within a transaction. Source timestamps are not used for ordering.

Rows and pending positions are held until commit. An interrupted transaction emits no buffered rows and retains the previous committed checkpoint. Memory is capped at 64 MiB of retained rows per transaction: after-images, plus the key-only record of each propagated delete, whose key values are copied so the before-image itself is not kept (see Delete propagation). Other before-images are discarded and do not count. Oversized transactions fail rather than skip data. Source fields are retained with each buffered row. Existing optional metadata remains available.

This candidate is restricted to the unsharded `-` shard. Successive positions must contain the previous GTID set. A new primary UUID is supported when its history extends the previous set; divergent or regressing histories and resharding fail. Deleted rows are dropped unless their table is listed in `delete_propagation_tables` (see Delete propagation below).

## Validation

Tests cover repeated updates, exact replay, several transactions in one response, transactions split across responses, interrupted commits, rollback, snapshot resume, CDC during copy, primary UUID changes, incomparable histories, and memory limits. Existing source tests also pass under Go 1.22.12.

Before production use, run an isolated Airbyte trial with a separate destination prefix. Verify discovered schema, generated BigQuery merge ordering, resumed VStream state, snapshot completion, and source/destination row comparisons. The production migration preserves existing GTID checkpoints and historical rows; rollout validation is recorded separately.

When deploying, retain the current GTID checkpoint, add the column through schema discovery, and select it as the destination cursor. Existing rows initially have a null ordering value; the first newer source event should supersede them. A historical repair requires a separately validated retain-records refresh. Rolling back the image also requires restoring the old configured cursor; never rewind the VStream state.

Live validation on 14 October: initial snapshot job21169 committed204467 rows across user/organization/member; resumed jobs21170 and21171 committed485 and226 records. All three BigQuery tables have valid ordering fields, no duplicate IDs or conversion errors. The283-row attribution cohort and200-row organization/member samples match source. Actual merge SQL orders by the new field, and decoded GTID checkpoints are nondecreasing. The final image is ordering-fbc60792e835d407fc2612077902a5a0e2e0e09e. Production rollout is recorded separately in the workspace.

Production validation on 14 October: job 21173 completed all 48 selected streams in 555 seconds, with 23,963 records emitted and committed. The 31 streams with new records had no missing ordering fields or conversion errors. Historical user IDs were retained and the 283-row attribution cohort had no mismatches. All 49 stored stream checkpoints were nondecreasing, including retained state for an unselected stream. Actual production merge SQL uses `_planetscale_order DESC NULLS LAST`. The original hourly schedule was restored. This validates future upsert ordering; it does not establish that every historical row in every table is current.

The preflight cursor lookup now propagates stream errors and rejects empty or invalid targets. Ordered mode checks that the target and committed GTID histories are comparable before the quiet-stream fast path. A lagging replica with an older, comparable target remains a safe no-op; an unrelated history fails without advancing the checkpoint. Regression tests ensure unavailable, empty, invalid and divergent targets fail rather than silently succeeding.

## Delete propagation

VStream reports a hard delete as a row change with a before-image and no after-image. The connector has always dropped those, so a row deleted in PlanetScale stays in the destination forever. That left Lab's erased Turnkey judge results and their rationale text in BigQuery (ENG-4102).

`delete_propagation_tables` is a comma-separated list of tables whose deletes are emitted instead. Each delete becomes a record holding only the row's primary key, `_ab_cdc_deleted_at` (the binlog event's timestamp, to the second, UTC; informational only) and the usual `_planetscale_order`. The other columns are left out so deleted content is not sent again. An append-dedup destination keyed on the primary key with `_planetscale_order` as cursor applies the record as a delete. The delete sorts after every earlier version of its row, including an insert in the same sync. An update that changes a listed row's primary key also emits a delete for the old key, ordered before the new row, so the old row cannot outlive a later erasure. Tables not in the list behave as before.

Guards, each of which fails the sync and keeps the committed checkpoint rather than dropping a delete:

- The option needs `ordered_cdc` and a primary key on each listed table, checked at discovery and at the start of each read.
- Discovery fails when a listed name is not a table, so a typo cannot silently turn deletion off.
- A read fails when a listed stream's configured catalog lacks `_ab_cdc_deleted_at` or `_planetscale_order`. Without the marker, the BigQuery destination applies a delete as an ordinary upsert and the checkpoint would move past it for good.
- A delete inside a snapshot chunk fails the transaction, because snapshot rows share one version and the delete could tie with the row it deletes. A real copy chunk never contains one.

Known window: the BigQuery destination's hard delete leaves no tombstone. If a sync commits a delete to BigQuery but fails before its state is saved, the retry replays from the earlier checkpoint. If that retry's stop position falls before the delete, for example on a lagging RDONLY tablet, it re-inserts the older version and the row is back until a later sync replays the delete. anam-dataform's `assert_lab_erased_judge_rows` reports any judge row that stays.

Rollout:

1. Build an image from this branch and run an isolated trial against a separate destination prefix, as for ordering. Delete a row in a scratch table and check it disappears after one sync. Check that a row inserted and deleted between syncs never appears, and that a sync with the option set but the old catalog fails.
2. Check the BigQuery destination version and its `cdc_deletion_mode`, and confirm the trial on that exact version. Use hard delete. Soft delete keeps a key-only tombstone with every other column null, which removes the content but adds rows that `engine_session` consumers would count unless they filter `_ab_cdc_deleted_at IS NULL`.
3. Pause the production connection and wait for any running job to finish. Set `delete_propagation_tables` to `turnkey_session_judge_result,turnkey_session_judge_bucket_result,engine_session`. Refresh the source schema, keeping the GTID state and without resetting or refreshing the streams' data. Confirm that each of the three streams is append-dedup on its source primary key with `_planetscale_order` as cursor, and has `_ab_cdc_deleted_at` selected. Then resume.
4. After the first successful sync with the option on, run anam-dataform's `scripts/lab-erasure-backfill.cjs` once more. That catches anything deleted between an earlier backfill and the cutover.
5. After the next real erasure, check that `assert_lab_erased_judge_rows` passes.

With `engine_session` listed, a Lab hard delete (the admin "fixes" action) removes the session from BigQuery too, including from the usage models that read it. anam-dataform's `docs/lab-erasure.md` records that decision. Other tables still drop deletes on purpose: analytics read historical rows that Lab later hard-deletes, for example users kept for signup attribution. Add a table only after checking its BigQuery consumers.
