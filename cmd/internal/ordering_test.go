package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"

	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"
)

const testSID = "e4e20f06-e28f-11ec-8d20-8e7ac09cb64c"

func pos(n int) string { return fmt.Sprintf("MySQL56/%s:1-%d", testSID, n) }
func vgtid(n int, pk *query.QueryResult) *binlogdata.VEvent {
	s := &binlogdata.ShardGtid{Keyspace: "db", Shard: "-", Gtid: pos(n)}
	if pk != nil {
		s.TablePKs = []*binlogdata.TableLastPK{{TableName: "users", Lastpk: pk}}
	}
	return &binlogdata.VEvent{Type: binlogdata.VEventType_VGTID, Vgtid: &binlogdata.VGtid{ShardGtids: []*binlogdata.ShardGtid{s}}}
}
func row(value string) *binlogdata.VEvent {
	return &binlogdata.VEvent{Type: binlogdata.VEventType_ROW, RowEvent: &binlogdata.RowEvent{RowChanges: []*binlogdata.RowChange{{After: &query.Row{Lengths: []int64{1, int64(len(value))}, Values: []byte("1" + value)}}}}}
}
func fields() *binlogdata.VEvent {
	return &binlogdata.VEvent{Type: binlogdata.VEventType_FIELD, FieldEvent: &binlogdata.FieldEvent{Fields: []*query.Field{{Name: "id", Type: query.Type_VARCHAR}, {Name: "status", Type: query.Type_VARCHAR}}}}
}
func commitEvent() *binlogdata.VEvent { return &binlogdata.VEvent{Type: binlogdata.VEventType_COMMIT} }
func decodeRecords(t *testing.T, b *bytes.Buffer) []AirbyteRecord {
	var result []AirbyteRecord
	d := json.NewDecoder(b)
	for d.More() {
		var m AirbyteMessage
		require.NoError(t, d.Decode(&m))
		if m.Record != nil {
			result = append(result, *m.Record)
		}
	}
	return result
}
func testTransaction(b *bytes.Buffer, n int) *orderedTransaction {
	return newOrderedTransaction(&psdbconnect.TableCursor{Keyspace: "db", Shard: "-", Position: pos(n)}, NewLogger(b), PlanetScaleSource{}, Stream{Name: "users", Namespace: "db"})
}
func TestOrderedRowsCommitReplayAndRepeatedUpdates(t *testing.T) {
	run := func() []AirbyteRecord {
		b := &bytes.Buffer{}
		tx := testTransaction(b, 10)
		for _, e := range []*binlogdata.VEvent{fields(), row("processing"), row("assessed"), vgtid(11, nil)} {
			require.NoError(t, tx.accept(e))
		}
		require.Zero(t, b.Len(), "no rows before commit")
		require.Equal(t, pos(10), tx.committed.Position, "no premature checkpoint")
		require.NoError(t, tx.accept(commitEvent()))
		require.Equal(t, pos(11), tx.committed.Position)
		return decodeRecords(t, b)
	}
	first, again := run(), run()
	require.Len(t, first, 2)
	require.Less(t, first[0].Data[orderingField], first[1].Data[orderingField])
	require.Equal(t, "assessed", first[1].Data["status"])
	for i := range first {
		require.Equal(t, first[i].Data[orderingField], again[i].Data[orderingField])
	}
}
func TestOrderedRowsMultipleTransactionsInOnePacket(t *testing.T) {
	b := &bytes.Buffer{}
	tx := testTransaction(b, 8)
	for _, e := range []*binlogdata.VEvent{fields(), row("old"), vgtid(9, nil), commitEvent(), row("new"), vgtid(10, nil), commitEvent()} {
		require.NoError(t, tx.accept(e))
	}
	records := decodeRecords(t, b)
	require.Len(t, records, 2)
	require.Less(t, records[0].Data[orderingField], records[1].Data[orderingField])
}
func TestOrderedSnapshotResumeDoesNotDependOnPacketOrdinal(t *testing.T) {
	pk := func(n string) *query.QueryResult {
		return &query.QueryResult{Rows: []*query.Row{{Lengths: []int64{1}, Values: []byte(n)}}}
	}
	run := func(prior *query.QueryResult, extra bool) string {
		b := &bytes.Buffer{}
		tx := testTransaction(b, 10)
		tx.committed.LastKnownPk = prior
		tx.pending.LastKnownPk = prior
		require.NoError(t, tx.accept(fields()))
		if extra {
			require.NoError(t, tx.accept(row("unrelated")))
		}
		require.NoError(t, tx.accept(row("snapshot")))
		require.NoError(t, tx.accept(vgtid(10, pk("9"))))
		require.NoError(t, tx.accept(commitEvent()))
		records := decodeRecords(t, b)
		return records[len(records)-1].Data[orderingField].(string)
	}
	require.Equal(t, run(nil, true), run(pk("8"), false))
}
func TestOrderedCDCWhileCopyingUsesTransactionOrdinal(t *testing.T) {
	b := &bytes.Buffer{}
	tx := testTransaction(b, 10)
	pk := &query.QueryResult{Rows: []*query.Row{{Values: []byte("5")}}}
	tx.committed.LastKnownPk = pk
	tx.pending.LastKnownPk = pk
	for _, e := range []*binlogdata.VEvent{fields(), row("old"), row("new"), vgtid(11, pk), commitEvent()} {
		require.NoError(t, tx.accept(e))
	}
	records := decodeRecords(t, b)
	require.Less(t, records[0].Data[orderingField], records[1].Data[orderingField])
}
func TestOrderingRejectsDivergenceAndOversizedTransactions(t *testing.T) {
	b := &bytes.Buffer{}
	tx := testTransaction(b, 10)
	require.Error(t, tx.accept(vgtid(9, nil)))
	require.Equal(t, pos(10), tx.committed.Position)
	require.NoError(t, tx.accept(fields()))
	tx.bytes = maxOrderedTransactionBytes
	require.Error(t, tx.accept(row("too large")))
	require.Zero(t, b.Len())
	require.Error(t, tx.accept(&binlogdata.VEvent{Type: binlogdata.VEventType_JOURNAL}))
}
func TestTransactionRankIncludesAllServersAndIntervals(t *testing.T) {
	rank, err := transactionRank("MySQL56/" + testSID + ":1-9:12-14,aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-2")
	require.NoError(t, err)
	require.EqualValues(t, 14, rank)
	for _, bad := range []string{"", "MySQL56/bad", "MariaDB/0-1-5"} {
		_, err := transactionRank(bad)
		require.Error(t, err)
	}
}

func TestOrderingRejectsEqualCardinalityDifferentHistory(t *testing.T) {
	b := &bytes.Buffer{}
	tx := testTransaction(b, 10)
	e := vgtid(10, nil)
	e.Vgtid.ShardGtids[0].Gtid = "MySQL56/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10"
	require.Error(t, tx.accept(e))
	require.Equal(t, pos(10), tx.committed.Position)
}

func TestOrderingContinuesAcrossPrimaryUUIDChange(t *testing.T) {
	b := &bytes.Buffer{}
	tx := testTransaction(b, 10)
	e := vgtid(10, nil)
	e.Vgtid.ShardGtids[0].Gtid = pos(10) + ",aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1"
	for _, event := range []*binlogdata.VEvent{fields(), row("after failover"), e, commitEvent()} {
		require.NoError(t, tx.accept(event))
	}
	records := decodeRecords(t, b)
	require.Equal(t, "00000000000000000011:0:00000000000000000001", records[0].Data[orderingField])
}

func TestOrderingRollbackDoesNotEmitOrAdvanceCheckpoint(t *testing.T) {
	b := &bytes.Buffer{}
	tx := testTransaction(b, 10)
	for _, event := range []*binlogdata.VEvent{fields(), row("discard"), vgtid(11, nil), {Type: binlogdata.VEventType_ROLLBACK}} {
		require.NoError(t, tx.accept(event))
	}
	require.Zero(t, b.Len())
	require.Empty(t, tx.rows)
	require.Equal(t, pos(10), tx.committed.Position)
}

func TestOrderingSnapshotWinsAtSamePositionAndNextCDCWins(t *testing.T) {
	b := &bytes.Buffer{}
	tx := testTransaction(b, 9)
	for _, event := range []*binlogdata.VEvent{fields(), row("transaction"), vgtid(10, nil), commitEvent(), row("snapshot"), vgtid(10, &query.QueryResult{Rows: []*query.Row{{Values: []byte("1")}}}), commitEvent(), row("later"), vgtid(11, nil), commitEvent()} {
		require.NoError(t, tx.accept(event))
	}
	records := decodeRecords(t, b)
	require.Len(t, records, 3)
	require.Less(t, records[0].Data[orderingField], records[1].Data[orderingField])
	require.Less(t, records[1].Data[orderingField], records[2].Data[orderingField])
}

func TestOrderingDiscoverySelectsVersionOnlyWhenEnabled(t *testing.T) {
	p := PlanetScaleEdgeDatabase{Mysql: &orderingSchemaMock{}}
	for _, enabled := range []bool{false, true} {
		stream, err := p.getStreamForTable(context.Background(), PlanetScaleSource{Database: "db", OrderedCDC: enabled}, "users")
		require.NoError(t, err)
		if enabled {
			require.Equal(t, []string{orderingField}, stream.DefaultCursorFields)
			require.Equal(t, []string{"string"}, stream.Schema.Properties[orderingField].Type)
		} else {
			_, exists := stream.Schema.Properties[orderingField]
			require.False(t, exists)
		}
	}
}

type orderingSchemaMock struct{ mysqlAccessMock }

func (orderingSchemaMock) GetTableSchema(context.Context, PlanetScaleSource, string) (map[string]PropertyType, error) {
	return map[string]PropertyType{"id": {Type: []string{"string"}}}, nil
}
func (orderingSchemaMock) GetTablePrimaryKeys(context.Context, PlanetScaleSource, string) ([]string, error) {
	return []string{"id"}, nil
}

func TestOrderingRejectsRegressionBeforeCommit(t *testing.T) {
	b := &bytes.Buffer{}
	tx := testTransaction(b, 10)
	require.NoError(t, tx.accept(vgtid(12, nil)))
	require.Error(t, tx.accept(vgtid(11, nil)))
	require.Equal(t, pos(10), tx.committed.Position)
}
