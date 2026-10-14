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
