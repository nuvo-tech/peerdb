package connpostgres

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

type recordingSnapshotSlotConnection struct {
	queries     []string
	failOnQuery string
}

func (c *recordingSnapshotSlotConnection) Exec(
	_ context.Context,
	query string,
	_ ...any,
) (pgconn.CommandTag, error) {
	c.queries = append(c.queries, query)
	if query == c.failOnQuery {
		return pgconn.CommandTag{}, errors.New("setting rejected")
	}
	return pgconn.CommandTag{}, nil
}

func TestConfigureSnapshotSlotConnectionDisablesSnapshotKillingTimeouts(
	t *testing.T,
) {
	t.Parallel()
	conn := &recordingSnapshotSlotConnection{}

	err := configureSnapshotSlotConnection(t.Context(), conn)

	require.NoError(t, err)
	require.Equal(t, []string{
		"SET idle_in_transaction_session_timeout=0",
		"SET lock_timeout=0",
		"SET wal_sender_timeout=0",
	}, conn.queries)
}

func TestConfigureSnapshotSlotConnectionReturnsSettingError(t *testing.T) {
	t.Parallel()
	conn := &recordingSnapshotSlotConnection{
		failOnQuery: "SET wal_sender_timeout=0",
	}

	err := configureSnapshotSlotConnection(t.Context(), conn)

	require.ErrorContains(t, err, "error setting wal_sender_timeout")
}
