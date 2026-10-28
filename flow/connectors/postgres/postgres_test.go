package connpostgres

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/PeerDB-io/peerdb/flow/alerting"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

func TestHandleSlotInfoRecordsHistoryBeforeFirstNormalize(t *testing.T) {
	ctx := t.Context()
	pool, err := internal.GetCatalogConnectionPoolFromEnv(ctx)
	require.NoError(t, err)
	conn, err := pgx.Connect(ctx, internal.GetCatalogConnectionStringFromEnv(ctx))
	require.NoError(t, err)
	defer conn.Close(context.Background())
	connector := &PostgresConnector{
		conn:   conn,
		Config: internal.GetCatalogPostgresConfigFromEnv(ctx),
		logger: internal.LoggerFromCtx(ctx),
	}

	slotName := "cadence_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	// A temporary physical slot exercises real slot polling without requiring
	// logical decoding configuration, and disappears when this connection closes.
	_, err = connector.conn.Exec(ctx, "SELECT pg_create_physical_replication_slot($1, true, true)", slotName)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := pool.Exec(context.Background(), "DELETE FROM peerdb_stats.peer_slot_size WHERE slot_name=$1", slotName)
		require.NoError(t, err)
	})

	meter := noop.NewMeterProvider().Meter("slot_history_test")
	intGauge, err := meter.Int64Gauge("slot")
	require.NoError(t, err)
	floatGauge, err := meter.Float64Gauge("slot_size")
	require.NoError(t, err)
	gauges := otel_metrics.SlotMetricGauges{
		SlotLagGauge:                    floatGauge,
		RestartLSNGauge:                 intGauge,
		ConfirmedFlushLSNGauge:          intGauge,
		SentLSNGauge:                    intGauge,
		CurrentWalLSNGauge:              intGauge,
		RestartToConfirmedMBGauge:       floatGauge,
		ConfirmedToCurrentMBGauge:       floatGauge,
		WalStatusGauge:                  intGauge,
		SafeWalSizeGauge:                intGauge,
		LogSpace:                        otel_metrics.LogSpaceGauges{UsedGauge: intGauge, LimitGauge: intGauge, SafeRatioGauge: floatGauge},
		SlotActiveGauge:                 intGauge,
		WalSenderStateGauge:             intGauge,
		StatsResetGauge:                 intGauge,
		SpillTxnsGauge:                  intGauge,
		SpillCountGauge:                 intGauge,
		SpillBytesGauge:                 intGauge,
		LogicalDecodingWorkMemGauge:     intGauge,
		OpenConnectionsGauge:            intGauge,
		OpenReplicationConnectionsGauge: intGauge,
		IntervalSinceLastNormalizeGauge: floatGauge,
	}
	keys := &alerting.AlertKeys{FlowName: slotName, PeerName: "test_peer", SlotName: slotName}
	require.NoError(t, connector.HandleSlotInfo(ctx, alerting.NewAlerter(ctx, pool, nil), pool, keys, gauges))

	var historyRows int
	require.NoError(t, pool.QueryRow(ctx,
		"SELECT count(*) FROM peerdb_stats.peer_slot_size WHERE slot_name=$1 AND peer_name=$2",
		slotName, keys.PeerName).Scan(&historyRows))
	require.Equal(t, 1, historyRows, "unknown normalization age must not suppress slot history")
}

func TestIntervalSinceLastNormalize(t *testing.T) {
	pool, err := internal.GetCatalogConnectionPoolFromEnv(t.Context())
	require.NoError(t, err)

	for _, test := range []struct {
		name        string
		completed   bool
		pending     bool
		expectedAge *time.Duration
	}{
		{name: "completed batch ignores quiet tables and newer unnormalized sync", completed: true, pending: true, expectedAge: new(2 * time.Hour)},
		{name: "first sync has not normalized", pending: true},
		{name: "no batches yet"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()
			tx, err := pool.Begin(ctx)
			require.NoError(t, err)
			defer tx.Rollback(context.Background())

			flowName := "normalize_cadence_" + uuid.NewString()
			otherFlowName := flowName + "_other"
			_, err = tx.Exec(ctx, `INSERT INTO peerdb_stats.cdc_flows
				(flow_name, latest_lsn_at_source, latest_lsn_at_target) VALUES ($1, 0, 0), ($2, 0, 0)`, flowName, otherFlowName)
			require.NoError(t, err)

			// A table can be quiet for months while another table has just synced.
			// Neither timestamp proves when a batch finished normalizing.
			_, err = tx.Exec(ctx, `INSERT INTO peerdb_stats.cdc_table_aggregate_counts
				(flow_name, destination_table_name, last_updated_at) VALUES
				($1, 'quiet_table', now() - interval '70 days'),
				($1, 'active_table', now() - interval '1 minute')`, flowName)
			require.NoError(t, err)

			_, err = tx.Exec(ctx, `INSERT INTO peerdb_stats.cdc_batches
				(flow_name, batch_id, rows_in_batch, batch_start_lsn, batch_end_lsn, start_time, end_time)
				VALUES ($1, 1, 1, 0, 1, now() - interval '1 minute', now() - interval '30 seconds')`, otherFlowName)
			require.NoError(t, err)
			if test.completed {
				_, err = tx.Exec(ctx, `INSERT INTO peerdb_stats.cdc_batches
					(flow_name, batch_id, rows_in_batch, batch_start_lsn, batch_end_lsn, start_time, end_time)
					VALUES ($1, 1, 1, 0, 1, now() - interval '4 hours', now() - interval '3 hours'),
					($1, 2, 1, 1, 2, now() - interval '3 hours', now() - interval '2 hours')`, flowName)
				require.NoError(t, err)
			}
			if test.pending {
				_, err = tx.Exec(ctx, `INSERT INTO peerdb_stats.cdc_batches
					(flow_name, batch_id, rows_in_batch, batch_start_lsn, batch_end_lsn, start_time, sync_time)
					VALUES ($1, 3, 1, 2, 3, now() - interval '1 minute', now() - interval '1 minute')`, flowName)
				require.NoError(t, err)
			}

			var age *time.Duration
			require.NoError(t, tx.QueryRow(ctx, intervalSinceLastNormalizeQuery, flowName).Scan(&age))
			require.Equal(t, test.expectedAge, age)
		})
	}
}

func TestNewPostgresConnectorAuthError(t *testing.T) {
	t.Parallel()
	config := internal.GetCatalogPostgresConfigFromEnv(t.Context())
	config.Password = "wrong_password"
	_, err := NewPostgresConnector(t.Context(), nil, config)
	require.Error(t, err)

	var authErr *exceptions.AuthError
	require.ErrorAs(t, err, &authErr)
}
