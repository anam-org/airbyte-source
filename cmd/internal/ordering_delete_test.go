package internal

import (
	"bytes"
	"context"
	"strings"
	"testing"

	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/proto/vtgateservice"
)

func deleteRow(value string, timestamp int64) *binlogdata.VEvent {
	return &binlogdata.VEvent{Type: binlogdata.VEventType_ROW, Timestamp: timestamp, RowEvent: &binlogdata.RowEvent{RowChanges: []*binlogdata.RowChange{{Before: &query.Row{Lengths: []int64{1, int64(len(value))}, Values: []byte("1" + value)}}}}}
}

func deleteTransaction(b *bytes.Buffer, source PlanetScaleSource) *orderedTransaction {
	source.OrderedCDC = true
	return newOrderedTransaction(&psdbconnect.TableCursor{Keyspace: "db", Shard: "-", Position: pos(10)}, NewLogger(b), source, Stream{Name: "users", Namespace: "db", PrimaryKeys: [][]string{{"id"}}})
}

func TestOrderedDeleteEmitsKeyOnlyTombstoneAfterEarlierVersions(t *testing.T) {
	b := &bytes.Buffer{}
	tx := deleteTransaction(b, PlanetScaleSource{DeleteTables: "orders, users"})
	for _, e := range []*binlogdata.VEvent{fields(), row("assessed"), deleteRow("assessed", 1793664000), vgtid(11, nil), commitEvent()} {
		require.NoError(t, tx.accept(e))
	}
	records := decodeRecords(t, b)
	require.Len(t, records, 2)
	require.NotContains(t, records[0].Data, deletedAtField)
	deletion := records[1].Data
	require.Equal(t, "2026-11-03T00:00:00Z", deletion[deletedAtField])
	require.Equal(t, "1", deletion["id"])
	require.NotContains(t, deletion, "status", "deleted content must not be re-sent")
	require.Less(t, records[0].Data[orderingField], deletion[orderingField], "the delete must win the destination dedup")
}

func TestOrderedDeleteIsDroppedForTablesNotOptedIn(t *testing.T) {
	for _, tables := range []string{"", "orders", "users_archive"} {
		b := &bytes.Buffer{}
		tx := deleteTransaction(b, PlanetScaleSource{DeleteTables: tables})
		for _, e := range []*binlogdata.VEvent{fields(), deleteRow("assessed", 1), vgtid(11, nil), commitEvent()} {
			require.NoError(t, tx.accept(e))
		}
		require.Empty(t, decodeRecords(t, b), tables)
		require.Equal(t, pos(11), tx.committed.Position, "a dropped delete still advances the checkpoint")
	}
}

func TestOrderedDeleteWithoutPrimaryKeyFailsWithoutAdvancing(t *testing.T) {
	b := &bytes.Buffer{}
	tx := newOrderedTransaction(&psdbconnect.TableCursor{Keyspace: "db", Shard: "-", Position: pos(10)}, NewLogger(b), PlanetScaleSource{OrderedCDC: true, DeleteTables: "users"}, Stream{Name: "users", Namespace: "db"})
	require.NoError(t, tx.accept(fields()))
	require.ErrorContains(t, tx.accept(deleteRow("assessed", 1)), "no primary key")
	require.Equal(t, pos(10), tx.committed.Position)
	require.Empty(t, decodeRecords(t, b))
}

func TestDeletionRecordKeepsEveryCompositeKeyColumn(t *testing.T) {
	record, err := deletionRecord(map[string]interface{}{"engine_session_id": "s1", "bucket_name": "tone", "rationale": "text"}, Stream{Name: "buckets", PrimaryKeys: [][]string{{"engine_session_id"}, {"bucket_name"}}}, "2026-11-03T00:00:00Z")
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{"engine_session_id": "s1", "bucket_name": "tone", deletedAtField: "2026-11-03T00:00:00Z"}, record)

	_, err = deletionRecord(map[string]interface{}{"engine_session_id": "s1"}, Stream{Name: "buckets", PrimaryKeys: [][]string{{"engine_session_id"}, {"bucket_name"}}}, "2026-11-03T00:00:00Z")
	require.Error(t, err)
}

func TestDeleteDiscoveryAddsMarkerOnlyForOptedInOrderedTables(t *testing.T) {
	p := PlanetScaleEdgeDatabase{Mysql: &orderingSchemaMock{}}
	for _, test := range []struct {
		source PlanetScaleSource
		marker bool
		fails  bool
	}{
		{source: PlanetScaleSource{Database: "db", OrderedCDC: true}},
		{source: PlanetScaleSource{Database: "db", OrderedCDC: true, DeleteTables: "orders"}},
		{source: PlanetScaleSource{Database: "db", OrderedCDC: true, DeleteTables: "orders,users"}, marker: true},
		{source: PlanetScaleSource{Database: "db", DeleteTables: "users"}, fails: true},
	} {
		stream, err := p.getStreamForTable(context.Background(), test.source, "users")
		if test.fails {
			require.Error(t, err)
			continue
		}
		require.NoError(t, err)
		marker, exists := stream.Schema.Properties[deletedAtField]
		require.Equal(t, test.marker, exists, test.source.DeleteTables)
		if exists {
			require.Equal(t, []string{"null", "string"}, marker.Type)
			require.Equal(t, "timestamp_with_timezone", marker.AirbyteType)
			require.Equal(t, []string{orderingField}, stream.DefaultCursorFields)
		}
	}
}

func TestReadRejectsDeletePropagationWithoutOrderedCDC(t *testing.T) {
	b := &bytes.Buffer{}
	ped := PlanetScaleEdgeDatabase{Logger: NewLogger(b)}
	vsc := &vstreamClientMock{vstreamFn: func(context.Context, *vtgate.VStreamRequest, ...grpc.CallOption) (vtgateservice.Vitess_VStreamClient, error) {
		return &vtgateVStreamClientMock{vstreamResponses: []*vstreamResponse{{response: &vtgate.VStreamResponse{Events: []*binlogdata.VEvent{vgtid(11, nil)}}}}}, nil
	}}
	ped.vtgateClientFn = func(context.Context, PlanetScaleSource) (vtgateservice.VitessClient, error) { return vsc, nil }
	_, err := ped.Read(context.Background(), b, PlanetScaleSource{DeleteTables: "users"}, ConfiguredStream{Stream: Stream{Name: "users", Namespace: "db"}, SyncMode: "incremental"}, &psdbconnect.TableCursor{Keyspace: "db", Shard: "-", Position: pos(10)})
	require.ErrorContains(t, err, "requires ordered_cdc")
}

func keyedRow(id, value string) *query.Row {
	return &query.Row{Lengths: []int64{int64(len(id)), int64(len(value))}, Values: []byte(id + value)}
}

func TestOrderedKeyChangeDeletesTheOldKeyFirst(t *testing.T) {
	b := &bytes.Buffer{}
	tx := deleteTransaction(b, PlanetScaleSource{DeleteTables: "users"})
	move := &binlogdata.VEvent{Type: binlogdata.VEventType_ROW, Timestamp: 1793664000, RowEvent: &binlogdata.RowEvent{RowChanges: []*binlogdata.RowChange{{Before: keyedRow("1", "tone"), After: keyedRow("2", "tone")}}}}
	same := &binlogdata.VEvent{Type: binlogdata.VEventType_ROW, RowEvent: &binlogdata.RowEvent{RowChanges: []*binlogdata.RowChange{{Before: keyedRow("2", "tone"), After: keyedRow("2", "quality")}}}}
	for _, e := range []*binlogdata.VEvent{fields(), move, same, vgtid(11, nil), commitEvent()} {
		require.NoError(t, tx.accept(e))
	}
	records := decodeRecords(t, b)
	require.Len(t, records, 3, "the in-place update emits no deletion")
	require.Equal(t, map[string]interface{}{"id": "1", deletedAtField: "2026-11-03T00:00:00Z", orderingField: records[0].Data[orderingField]}, records[0].Data)
	require.Equal(t, "2", records[1].Data["id"])
	require.Less(t, records[0].Data[orderingField], records[1].Data[orderingField])
	require.Equal(t, "quality", records[2].Data["status"])
}

func TestOrderedKeyChangeIsUnchangedForTablesNotOptedIn(t *testing.T) {
	b := &bytes.Buffer{}
	tx := deleteTransaction(b, PlanetScaleSource{})
	move := &binlogdata.VEvent{Type: binlogdata.VEventType_ROW, RowEvent: &binlogdata.RowEvent{RowChanges: []*binlogdata.RowChange{{Before: keyedRow("1", "tone"), After: keyedRow("2", "tone")}}}}
	for _, e := range []*binlogdata.VEvent{fields(), move, vgtid(11, nil), commitEvent()} {
		require.NoError(t, tx.accept(e))
	}
	records := decodeRecords(t, b)
	require.Len(t, records, 1)
	require.NotContains(t, records[0].Data, deletedAtField)
}

func TestOrderedDeleteInsideSnapshotChunkFailsWithoutAdvancing(t *testing.T) {
	b := &bytes.Buffer{}
	tx := deleteTransaction(b, PlanetScaleSource{DeleteTables: "users"})
	pk := &query.QueryResult{Rows: []*query.Row{{Lengths: []int64{1}, Values: []byte("1")}}}
	for _, e := range []*binlogdata.VEvent{fields(), row("assessed"), deleteRow("assessed", 1), vgtid(10, pk)} {
		require.NoError(t, tx.accept(e))
	}
	require.ErrorContains(t, tx.accept(commitEvent()), "snapshot chunk")
	require.Equal(t, pos(10), tx.committed.Position)
	require.Nil(t, tx.committed.LastKnownPk)
	require.Empty(t, decodeRecords(t, b))
}

func TestDeletePropagationNeedsPrimaryKeyAndConfiguredMarker(t *testing.T) {
	source := PlanetScaleSource{OrderedCDC: true, DeleteTables: "users"}
	marked := StreamSchema{Properties: map[string]PropertyType{"id": {}, deletedAtField: {}, orderingField: {}}}
	require.NoError(t, validateDeletePropagation(source, Stream{Name: "users", PrimaryKeys: [][]string{{"id"}}, Schema: marked}, true))
	require.ErrorContains(t, validateDeletePropagation(source, Stream{Name: "users", Schema: marked}, true), "primary key")
	for _, missing := range []string{deletedAtField, orderingField} {
		schema := StreamSchema{Properties: map[string]PropertyType{"id": {}, deletedAtField: {}, orderingField: {}}}
		delete(schema.Properties, missing)
		require.ErrorContains(t, validateDeletePropagation(source, Stream{Name: "users", PrimaryKeys: [][]string{{"id"}}, Schema: schema}, true), missing)
	}
	require.NoError(t, validateDeletePropagation(source, Stream{Name: "users", PrimaryKeys: [][]string{{"id"}}}, false), "discovery adds the marker itself")
}

func TestReadRejectsStaleCatalogBeforeTouchingTheSource(t *testing.T) {
	b := &bytes.Buffer{}
	ped := PlanetScaleEdgeDatabase{Logger: NewLogger(b)}
	ped.vtgateClientFn = func(context.Context, PlanetScaleSource) (vtgateservice.VitessClient, error) {
		t.Fatal("the source must not be read with a stale catalog")
		return nil, nil
	}
	stale := Stream{Name: "users", Namespace: "db", PrimaryKeys: [][]string{{"id"}}, Schema: StreamSchema{Properties: map[string]PropertyType{"id": {}, orderingField: {}}}}
	tc := &psdbconnect.TableCursor{Keyspace: "db", Shard: "-", Position: pos(10)}
	_, err := ped.Read(context.Background(), b, PlanetScaleSource{OrderedCDC: true, DeleteTables: "users"}, ConfiguredStream{Stream: stale, SyncMode: "incremental"}, tc)
	require.ErrorContains(t, err, deletedAtField)
	require.Equal(t, pos(10), tc.Position)
}

type namedTablesMock struct{ orderingSchemaMock }

func (namedTablesMock) GetTableNames(context.Context, PlanetScaleSource) ([]string, error) {
	return []string{"users"}, nil
}

func TestDiscoveryRejectsUnknownDeleteTables(t *testing.T) {
	p := PlanetScaleEdgeDatabase{Mysql: &namedTablesMock{}}
	_, err := p.DiscoverSchema(context.Background(), PlanetScaleSource{Database: "db", OrderedCDC: true, DeleteTables: "users, user"})
	require.ErrorContains(t, err, "lists user, which is not a table")
	catalog, err := p.DiscoverSchema(context.Background(), PlanetScaleSource{Database: "db", OrderedCDC: true, DeleteTables: " users ,"})
	require.NoError(t, err)
	require.Contains(t, catalog.Streams[0].Schema.Properties, deletedAtField)
}

func TestOrderedDeletesBufferOnlyTheirKeys(t *testing.T) {
	b := &bytes.Buffer{}
	tx := deleteTransaction(b, PlanetScaleSource{DeleteTables: "users"})
	huge := strings.Repeat("x", maxOrderedTransactionBytes/2)
	require.NoError(t, tx.accept(fields()))
	// Three deletes whose before-images together pass the cap: a bulk erasure of large
	// rows must not stall the stream the way the 12 Nov scrub did.
	for i := 0; i < 3; i++ {
		require.NoError(t, tx.accept(deleteRow(huge, 1)))
	}
	require.Less(t, tx.bytes, 1024)
	require.NoError(t, tx.accept(vgtid(11, nil)))
	require.NoError(t, tx.accept(commitEvent()))
	records := decodeRecords(t, b)
	require.Len(t, records, 3)
	require.NotContains(t, records[0].Data, "status")
}
