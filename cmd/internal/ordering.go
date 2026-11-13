package internal

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"google.golang.org/protobuf/proto"
	vtmysql "vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"
)

const orderingField = "_planetscale_order"

// Airbyte's CDC deletion marker. In append-dedup mode the destination deletes the
// row whose primary key a record carrying this field names.
const deletedAtField = "_ab_cdc_deleted_at"

// Bounds the rows buffered for one transaction: after-images, and for a delete the
// key-only record it becomes. Discarded before-images do not count: a bulk scrub
// of large JSON columns carries big before-images but small after-images (13 Nov
// 2026: a 4,974-row backfill stalled engine_session for a day because its
// before-images passed 64 MiB).
const maxOrderedTransactionBytes = 64 << 20

// GTID set cardinality increases along one shard's replication history, including
// primary failover to a new UUID. It is not an order across unrelated shards.
func transactionRank(position string) (uint64, error) {
	p, err := vtmysql.DecodePosition(position)
	if err != nil || p.GTIDSet == nil {
		return 0, fmt.Errorf("invalid ordering position")
	}
	if p.GTIDSet.Flavor() != "MySQL56" {
		return 0, fmt.Errorf("ordering requires MySQL56 positions")
	}
	var count uint64
	for _, server := range strings.Split(p.GTIDSet.String(), ",") {
		parts := strings.Split(server, ":")
		for _, interval := range parts[1:] {
			bounds := strings.Split(interval, "-")
			start, err := strconv.ParseUint(bounds[0], 10, 64)
			if err != nil || start == 0 {
				return 0, fmt.Errorf("invalid GTID interval")
			}
			end := start
			if len(bounds) == 2 {
				end, err = strconv.ParseUint(bounds[1], 10, 64)
			}
			if err != nil || end < start || count > math.MaxUint64-(end-start+1) {
				return 0, fmt.Errorf("GTID cardinality overflow")
			}
			count += end - start + 1
		}
	}
	return count, nil
}

// A row version held until its transaction commits: an after-image, or for a
// delete the key-only record that replaces it.
type orderedRow struct {
	result   *query.QueryResult
	deletion map[string]interface{}
}

type orderedTransaction struct {
	committed *psdbconnect.TableCursor
	pending   *psdbconnect.TableCursor
	fields    []*query.Field
	rows      []orderedRow
	bytes     int
	logger    AirbyteLogger
	source    PlanetScaleSource
	stream    Stream
	count     int
}

func newOrderedTransaction(tc *psdbconnect.TableCursor, logger AirbyteLogger, source PlanetScaleSource, stream Stream) *orderedTransaction {
	return &orderedTransaction{committed: tc, pending: proto.Clone(tc).(*psdbconnect.TableCursor), logger: logger, source: source, stream: stream}
}

func (o *orderedTransaction) accept(event *binlogdata.VEvent) error {
	switch event.Type {
	case binlogdata.VEventType_FIELD:
		for _, f := range event.FieldEvent.Fields {
			if f.Name == orderingField {
				return fmt.Errorf("source column conflicts with %s", orderingField)
			}
		}
		o.fields = event.FieldEvent.Fields
	case binlogdata.VEventType_ROW:
		if len(o.fields) == 0 {
			return fmt.Errorf("row before field metadata")
		}
		propagates := o.source.PropagatesDeletes(o.stream.Name)
		for _, change := range event.RowEvent.RowChanges {
			deleteBefore := propagates && change.Before != nil && change.After == nil
			// An update that moves a row to a new primary key leaves the old key's
			// row in the destination unless the old key is deleted first.
			if propagates && change.Before != nil && change.After != nil {
				moved, err := o.keyChanged(change.Before, change.After)
				if err != nil {
					return err
				}
				deleteBefore = moved
			}
			// A delete is reduced to its key-only record now, so its before-image
			// is not buffered: deleting many large rows cannot hit the cap.
			if deleteBefore {
				deletion, err := o.deletion(change.Before, eventTime(event))
				if err != nil {
					return err
				}
				if err := o.retain(orderedRow{deletion: deletion}, deletionSize(deletion)); err != nil {
					return err
				}
			}
			if change.After == nil {
				continue
			}
			if err := o.retain(orderedRow{result: &query.QueryResult{Fields: o.fields, Rows: []*query.Row{change.After}}}, proto.Size(change.After)); err != nil {
				return err
			}
		}
	case binlogdata.VEventType_VGTID:
		if len(event.GetVgtid().GetShardGtids()) != 1 {
			return fmt.Errorf("ordered mode requires exactly one shard")
		}
		shard := event.Vgtid.ShardGtids[0]
		if shard.Shard != o.committed.Shard || shard.Keyspace != o.committed.Keyspace {
			return fmt.Errorf("shard identity changed; ordering cannot be compared")
		}
		if _, err := transactionRank(shard.Gtid); err != nil {
			return err
		}
		if o.pending.Position != "" {
			previous, err := vtmysql.DecodePosition(o.pending.Position)
			if err != nil {
				return err
			}
			next, _ := vtmysql.DecodePosition(shard.Gtid)
			if !next.AtLeast(previous) {
				return fmt.Errorf("GTID history regressed or diverged; checkpoint retained")
			}
		}
		o.pending.Position = shard.Gtid
		o.pending.LastKnownPk = nil
		for _, pk := range shard.TablePKs {
			if pk.TableName == o.stream.Name {
				o.pending.LastKnownPk = pk.Lastpk
			}
		}
	case binlogdata.VEventType_LASTPK:
		if event.GetLastPKEvent().GetTableLastPK() != nil {
			o.pending.LastKnownPk = event.LastPKEvent.TableLastPK.Lastpk
		}
	case binlogdata.VEventType_COMMIT, binlogdata.VEventType_DDL, binlogdata.VEventType_OTHER:
		return o.commit()
	case binlogdata.VEventType_COPY_COMPLETED:
		if len(o.rows) != 0 {
			return fmt.Errorf("copy completed with uncommitted rows")
		}
		if err := o.commit(); err != nil {
			return err
		}
	case binlogdata.VEventType_JOURNAL:
		return fmt.Errorf("resharding requires an explicit ordering migration")
	case binlogdata.VEventType_ROLLBACK:
		o.rows = nil
		o.bytes = 0
		o.pending = proto.Clone(o.committed).(*psdbconnect.TableCursor)
	}
	return nil
}

func (o *orderedTransaction) commit() error {
	if o.pending.Position == "" && len(o.rows) == 0 {
		return nil
	}
	rank, err := transactionRank(o.pending.Position)
	if err != nil {
		return err
	}
	snapshotChunk := o.pending.LastKnownPk != nil && !proto.Equal(o.pending.LastKnownPk, o.committed.LastKnownPk)
	if len(o.rows) > 0 && !snapshotChunk && o.pending.Position == o.committed.Position {
		return fmt.Errorf("CDC rows have no advancing committed GTID")
	}
	// Snapshot rows share one version, so a delete among them could tie with the
	// row it deletes. A real copy chunk never contains one.
	if snapshotChunk {
		for _, r := range o.rows {
			if r.deletion != nil {
				return fmt.Errorf("delete inside a snapshot chunk of %s; checkpoint retained", o.stream.Name)
			}
		}
	}
	for i, r := range o.rows {
		phase := 0
		ordinal := i + 1
		// A snapshot represents the state after all transactions in its GTID set.
		// Its version must not depend on snapshot packet size or resume boundary.
		if snapshotChunk {
			phase = 1
			ordinal = 0
		}
		records := []map[string]interface{}{r.deletion}
		if r.deletion == nil {
			records = QueryResultToRecords(sqltypes.Proto3ToResult(r.result), &o.source)
		}
		for _, record := range records {
			record[orderingField] = fmt.Sprintf("%020d:%d:%020d", rank, phase, ordinal)
			if o.source.IncludeMetadata {
				record["_planetscale_metadata"] = map[string]interface{}{"vgtid_position": o.pending.Position, "extracted_at": time.Now().UnixNano(), "sequence_number": i + 1}
			}
			o.logger.Record(o.stream.Namespace, o.stream.Name, record)
			o.count++
		}
	}
	o.logger.Flush()
	proto.Reset(o.committed)
	proto.Merge(o.committed, o.pending)
	o.rows = nil
	o.bytes = 0
	return nil
}

// Deletes need ordered CDC: the deletion must sort after every earlier version of
// its row in the destination's dedup, and only _planetscale_order guarantees that.
// The destination needs the primary key to find the row. When configured is set,
// the stream comes from the configured catalog, which must carry the deletion
// marker and the ordering field: without them the destination applies a delete as
// an ordinary upsert, and the checkpoint moves past it for good.
func validateDeletePropagation(psc PlanetScaleSource, stream Stream, configured bool) error {
	if !psc.OrderedCDC {
		return fmt.Errorf("delete propagation for %s requires ordered_cdc", stream.Name)
	}
	if len(stream.PrimaryKeys) == 0 {
		return fmt.Errorf("delete propagation for %s requires a primary key", stream.Name)
	}
	if configured {
		for _, field := range []string{deletedAtField, orderingField} {
			if _, ok := stream.Schema.Properties[field]; !ok {
				return fmt.Errorf("delete propagation for %s: the configured catalog lacks %s; refresh the source schema and select it", stream.Name, field)
			}
		}
	}
	return nil
}

// Buffers one row for commit, counting size toward the transaction cap.
func (o *orderedTransaction) retain(row orderedRow, size int) error {
	o.bytes += size
	if o.bytes > maxOrderedTransactionBytes {
		return fmt.Errorf("ordered transaction exceeds 64 MiB of retained rows; checkpoint retained")
	}
	o.rows = append(o.rows, row)
	return nil
}

// The key-only deletion record for a before-image.
func (o *orderedTransaction) deletion(before *query.Row, at time.Time) (map[string]interface{}, error) {
	row := QueryResultToRecords(sqltypes.Proto3ToResult(&query.QueryResult{Fields: o.fields, Rows: []*query.Row{before}}), &o.source)[0]
	return deletionRecord(row, o.stream, at.UTC().Format(time.RFC3339Nano))
}

// What a deletion record holds in memory, roughly: a fixed cost for the map, its
// boxed values and the timestamp (about 450 bytes measured), plus its keys and values.
func deletionSize(record map[string]interface{}) int {
	size := 512
	for key, value := range record {
		size += len(key) + len(fmt.Sprint(value))
	}
	return size
}

// Whether an update's before- and after-images have different primary keys.
func (o *orderedTransaction) keyChanged(before, after *query.Row) (bool, error) {
	record := func(row *query.Row) map[string]interface{} {
		return QueryResultToRecords(sqltypes.Proto3ToResult(&query.QueryResult{Fields: o.fields, Rows: []*query.Row{row}}), &o.source)[0]
	}
	if len(o.stream.PrimaryKeys) == 0 {
		return false, fmt.Errorf("cannot propagate deletes from %s: the stream has no primary key", o.stream.Name)
	}
	b, a := record(before), record(after)
	for _, key := range o.stream.PrimaryKeys {
		if len(key) != 1 {
			return false, fmt.Errorf("cannot propagate deletes from %s: nested primary key %v", o.stream.Name, key)
		}
		if fmt.Sprint(b[key[0]]) != fmt.Sprint(a[key[0]]) {
			return true, nil
		}
	}
	return false, nil
}

// The commit time of the binlog event, falling back to the read time for events
// that carry none.
func eventTime(event *binlogdata.VEvent) time.Time {
	if event.Timestamp > 0 {
		return time.Unix(event.Timestamp, 0)
	}
	return time.Now()
}

// A deletion carries only the primary key and the marker. The before-image's
// other columns are dropped so deleted content is not sent to the destination
// again.
func deletionRecord(row map[string]interface{}, stream Stream, deletedAt string) (map[string]interface{}, error) {
	if len(stream.PrimaryKeys) == 0 {
		return nil, fmt.Errorf("cannot propagate a delete from %s: the stream has no primary key", stream.Name)
	}
	record := map[string]interface{}{deletedAtField: deletedAt}
	for _, key := range stream.PrimaryKeys {
		if len(key) != 1 {
			return nil, fmt.Errorf("cannot propagate a delete from %s: nested primary key %v", stream.Name, key)
		}
		value, ok := row[key[0]]
		if !ok || value == nil {
			return nil, fmt.Errorf("cannot propagate a delete from %s: before-image lacks primary key %s", stream.Name, key[0])
		}
		// A parsed value is a slice of the whole before-image. Copy it, or the
		// buffered record keeps every deleted row's content alive in memory.
		if v, ok := value.(sqltypes.Value); ok {
			value = sqltypes.MakeTrusted(v.Type(), bytes.Clone(v.Raw()))
		}
		record[key[0]] = value
	}
	return record, nil
}

// A lagging replica may have an older comparable position. An unrelated history
// must not be mistaken for an idle source by the no-new-GTIDs fast path.
func validateOrderingTarget(current, target string) error {
	if _, err := transactionRank(target); err != nil {
		return fmt.Errorf("invalid ordering target: %w", err)
	}
	if current == "" {
		return nil
	}
	if _, err := transactionRank(current); err != nil {
		return fmt.Errorf("invalid committed ordering position: %w", err)
	}
	previous, _ := vtmysql.DecodePosition(current)
	next, _ := vtmysql.DecodePosition(target)
	if !next.AtLeast(previous) && !previous.AtLeast(next) {
		return fmt.Errorf("stop cursor history diverged from committed checkpoint")
	}
	return nil
}
