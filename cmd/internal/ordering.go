package internal

import (
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

type orderedTransaction struct {
	committed *psdbconnect.TableCursor
	pending   *psdbconnect.TableCursor
	fields    []*query.Field
	rows      []*query.QueryResult
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
		o.bytes += proto.Size(event)
		if o.bytes > maxOrderedTransactionBytes {
			return fmt.Errorf("ordered transaction exceeds 64 MiB; checkpoint retained")
		}
		if len(o.fields) == 0 {
			return fmt.Errorf("row before field metadata")
		}
		for _, change := range event.RowEvent.RowChanges {
			if change.After != nil {
				o.rows = append(o.rows, &query.QueryResult{Fields: o.fields, Rows: []*query.Row{change.After}})
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
		if o.committed.Position != "" {
			previous, err := vtmysql.DecodePosition(o.committed.Position)
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
	for i, qr := range o.rows {
		phase := 0
		ordinal := i + 1
		// A snapshot represents the state after all transactions in its GTID set.
		// Its version must not depend on snapshot packet size or resume boundary.
		if snapshotChunk {
			phase = 1
			ordinal = 0
		}
		for _, record := range QueryResultToRecords(sqltypes.Proto3ToResult(qr), &o.source) {
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
