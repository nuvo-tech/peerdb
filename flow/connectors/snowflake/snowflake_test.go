package connsnowflake

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestNormalizedTablesEnableChangeTracking(t *testing.T) {
	table := &common.QualifiedTable{Namespace: "public", Table: "orders"}
	tableSchema := &protos.TableSchema{
		Columns:           []*protos.FieldDescription{{Name: "id", Type: string(types.QValueKindInt64)}},
		PrimaryKeyColumns: []string{"id"},
	}

	for _, isResync := range []bool{false, true} {
		t.Run(map[bool]string{false: "initial table", true: "resync replacement"}[isResync], func(t *testing.T) {
			query := generateCreateTableSQLForNormalizedTable(
				t.Context(),
				&protos.SetupNormalizedTableBatchInput{IsResync: isResync},
				table,
				tableSchema,
			)

			require.True(t, strings.HasSuffix(query, "CHANGE_TRACKING = TRUE"), query)
		})
	}
}
