package internal

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/proto/vtgateservice"
)

func TestOrderedSyncAcrossPacketsAndInterruptedCommit(t *testing.T) {
	for _, finish := range []bool{false, true} {
		t.Run(map[bool]string{false: "interrupted", true: "committed"}[finish], func(t *testing.T) {
			b := &bytes.Buffer{}
			ped := PlanetScaleEdgeDatabase{Logger: NewLogger(b)}
			responses := []*vstreamResponse{{response: &vtgate.VStreamResponse{Events: []*binlogdata.VEvent{fields(), row("old")}}}, {response: &vtgate.VStreamResponse{Events: []*binlogdata.VEvent{row("latest"), vgtid(11, nil)}}}}
			if finish {
				responses = append(responses, &vstreamResponse{response: &vtgate.VStreamResponse{Events: []*binlogdata.VEvent{commitEvent()}}})
			}
			client := &vtgateVStreamClientMock{vstreamResponses: responses}
			vsc := &vstreamClientMock{vstreamFn: func(context.Context, *vtgate.VStreamRequest, ...grpc.CallOption) (vtgateservice.Vitess_VStreamClient, error) {
				return client, nil
			}}
			ped.vtgateClientFn = func(context.Context, PlanetScaleSource) (vtgateservice.VitessClient, error) { return vsc, nil }
			tc := &psdbconnect.TableCursor{Keyspace: "db", Shard: "-", Position: pos(10)}
			_, count, err := ped.syncOrdered(context.Background(), "incremental", tc, pos(11), Stream{Name: "users", Namespace: "db"}, PlanetScaleSource{OrderedCDC: true}, psdbconnect.TabletType_primary, time.Minute)
			if finish {
				require.ErrorIs(t, err, io.EOF)
				require.Equal(t, 2, count)
				require.Equal(t, pos(11), tc.Position)
				require.Len(t, decodeRecords(t, b), 2)
			} else {
				require.Error(t, err)
				require.NotErrorIs(t, err, io.EOF)
				require.Zero(t, count)
				require.Zero(t, b.Len())
				require.Equal(t, pos(10), tc.Position)
			}
		})
	}
}

func TestOrderedReadResumesCopyWithoutNewTransactions(t *testing.T) {
	b := &bytes.Buffer{}
	ped := PlanetScaleEdgeDatabase{Logger: NewLogger(b)}
	firstPK := &query.QueryResult{Rows: []*query.Row{{Lengths: []int64{1}, Values: []byte("0")}}}
	nextPK := &query.QueryResult{Rows: []*query.Row{{Lengths: []int64{1}, Values: []byte("1")}}}
	calls := 0
	vsc := &vstreamClientMock{vstreamFn: func(_ context.Context, req *vtgate.VStreamRequest, _ ...grpc.CallOption) (vtgateservice.Vitess_VStreamClient, error) {
		calls++
		events := []*binlogdata.VEvent{vgtid(10, nil)}
		if calls > 1 {
			require.Equal(t, pos(10), req.Vgtid.ShardGtids[0].Gtid)
			require.NotEmpty(t, req.Vgtid.ShardGtids[0].TablePKs)
			events = []*binlogdata.VEvent{fields(), row("remaining"), vgtid(10, nextPK), commitEvent(), vgtid(10, nil), {Type: binlogdata.VEventType_COPY_COMPLETED}}
		}
		return &vtgateVStreamClientMock{vstreamResponses: []*vstreamResponse{{response: &vtgate.VStreamResponse{Events: events}}}}, nil
	}}
	ped.vtgateClientFn = func(context.Context, PlanetScaleSource) (vtgateservice.VitessClient, error) { return vsc, nil }
	tc := &psdbconnect.TableCursor{Keyspace: "db", Shard: "-", Position: pos(10), LastKnownPk: firstPK}
	_, err := ped.Read(context.Background(), b, PlanetScaleSource{OrderedCDC: true}, ConfiguredStream{Stream: Stream{Name: "users", Namespace: "db"}, SyncMode: "incremental"}, tc)
	require.NoError(t, err)
	require.Equal(t, 2, calls, "same GTID must not skip unfinished snapshot")
	require.Len(t, decodeRecords(t, b), 1)
	require.Nil(t, tc.LastKnownPk)
}

func TestOrderedReadChecksStopPositionBeforeQuietExit(t *testing.T) {
	for _, test := range []struct {
		name      string
		position  string
		streamErr error
		wantError bool
	}{
		{name: "unavailable", streamErr: io.ErrUnexpectedEOF, wantError: true},
		{name: "empty", position: "", wantError: true},
		{name: "invalid", position: "invalid", wantError: true},
		{name: "divergent", position: "MySQL56/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-10", wantError: true},
		{name: "equal", position: pos(10)},
		{name: "replica behind", position: pos(9)},
	} {
		t.Run(test.name, func(t *testing.T) {
			b := &bytes.Buffer{}
			ped := PlanetScaleEdgeDatabase{Logger: NewLogger(b)}
			calls := 0
			vsc := &vstreamClientMock{vstreamFn: func(context.Context, *vtgate.VStreamRequest, ...grpc.CallOption) (vtgateservice.Vitess_VStreamClient, error) {
				calls++
				if test.streamErr != nil {
					return nil, test.streamErr
				}
				event := vgtid(10, nil)
				event.Vgtid.ShardGtids[0].Gtid = test.position
				return &vtgateVStreamClientMock{vstreamResponses: []*vstreamResponse{{response: &vtgate.VStreamResponse{Events: []*binlogdata.VEvent{event}}}}}, nil
			}}
			ped.vtgateClientFn = func(context.Context, PlanetScaleSource) (vtgateservice.VitessClient, error) { return vsc, nil }
			tc := &psdbconnect.TableCursor{Keyspace: "db", Shard: "-", Position: pos(10)}
			_, err := ped.Read(context.Background(), b, PlanetScaleSource{OrderedCDC: true}, ConfiguredStream{Stream: Stream{Name: "users", Namespace: "db"}, SyncMode: "incremental"}, tc)
			if test.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, 1, calls)
			require.Equal(t, pos(10), tc.Position)
			require.Empty(t, decodeRecords(t, b))
		})
	}
}
