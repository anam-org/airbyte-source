package internal

import (
	"context"
	"fmt"
	"io"
	"time"

	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"vitess.io/vitess/go/vt/proto/binlogdata"
)

func (p PlanetScaleEdgeDatabase) syncOrdered(ctx context.Context, mode string, tc *psdbconnect.TableCursor, stop string, stream Stream, source PlanetScaleSource, tablet psdbconnect.TabletType, timeout time.Duration) (*psdbconnect.TableCursor, int, error) {
	// Resharded histories are not globally comparable by GTID cardinality.
	if tc.Shard != "-" {
		return tc, 0, fmt.Errorf("ordered CDC currently supports only the unsharded '-' shard")
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	client, conn, err := p.initializeVTGateClient(ctx, source)
	if err != nil {
		return tc, 0, err
	}
	if conn != nil {
		defer conn.Close()
	}
	request := buildVStreamRequest(tablet, stream.Name, tc.Shard, tc.Keyspace, tc.Position, tc.LastKnownPk)
	c, err := client.VStream(ctx, request)
	if err != nil {
		return tc, 0, err
	}
	if stream.Namespace == "" {
		stream.Namespace = source.Database
	}
	tx := newOrderedTransaction(tc, p.Logger, source, stream)
	for {
		response, err := c.Recv()
		if err != nil {
			if len(tx.rows) > 0 {
				return tc, tx.count, fmt.Errorf("VStream ended inside a transaction; committed checkpoint retained: %v", err)
			}
			return tc, tx.count, err
		}
		for _, event := range response.Events {
			if err := tx.accept(event); err != nil {
				return tc, tx.count, err
			}
			if mode == "full" && event.Type == binlogdata.VEventType_COPY_COMPLETED {
				return tc, tx.count, io.EOF
			}
			if mode != "full" && len(tx.rows) == 0 && (event.Type == binlogdata.VEventType_COMMIT || event.Type == binlogdata.VEventType_DDL || event.Type == binlogdata.VEventType_OTHER) && (positionAfter(tc.Position, stop) || positionEqual(tc.Position, stop)) {
				return tc, tx.count, io.EOF
			}
		}
	}
}
