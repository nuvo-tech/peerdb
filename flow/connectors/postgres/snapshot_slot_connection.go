package connpostgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
)

type snapshotSlotConnection interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

func configureSnapshotSlotConnection(ctx context.Context, conn snapshotSlotConnection) error {
	settings := []struct {
		query string
		name  string
	}{
		{"SET idle_in_transaction_session_timeout=0", "idle_in_transaction_session_timeout"},
		{"SET lock_timeout=0", "lock_timeout"},
		// CREATE_REPLICATION_SLOT exports a snapshot that remains valid only while
		// this otherwise-idle connection stays open.
		{"SET wal_sender_timeout=0", "wal_sender_timeout"},
	}
	for _, setting := range settings {
		if _, err := conn.Exec(ctx, setting.query); err != nil {
			return fmt.Errorf("[slot] error setting %s: %w", setting.name, err)
		}
	}
	return nil
}
