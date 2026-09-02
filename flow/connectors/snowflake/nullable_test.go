package connsnowflake

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestGenerateNormalizedTableKeepsSnowflakeUserColumnsNullable(t *testing.T) {
	t.Parallel()

	statement := generateCreateTableSQLForNormalizedTable(
		context.Background(),
		&protos.SetupNormalizedTableBatchInput{
			SoftDeleteColName: "_PEERDB_IS_DELETED",
			SyncedAtColName:   "_PEERDB_SYNCED_AT",
		},
		&common.QualifiedTable{Namespace: "public", Table: "events"},
		&protos.TableSchema{
			Columns: []*protos.FieldDescription{
				{Name: "id", Type: string(types.QValueKindInt64), Nullable: false},
				{Name: "description", Type: string(types.QValueKindString), Nullable: true},
			},
			PrimaryKeyColumns: []string{"id"},
			NullableEnabled:   true,
		},
	)

	require.NotContains(t, statement, "NOT NULL")
	require.Contains(t, statement, `"ID" INTEGER`)
	require.Contains(t, statement, `"DESCRIPTION" STRING`)
	require.Contains(t, statement, `_PEERDB_IS_DELETED BOOLEAN DEFAULT FALSE`)
	require.Contains(t, createRawTableSQL, "_PEERDB_UID STRING NOT NULL")
}

func TestGenerateMergeStatementKeepsSnowflakeCastsNullable(t *testing.T) {
	t.Parallel()

	destinationTable := "public.events"
	generator := &mergeStmtGenerator{
		tableSchemaMapping: map[string]*protos.TableSchema{
			destinationTable: {
				Columns: []*protos.FieldDescription{
					{Name: "id", Type: string(types.QValueKindInt64), Nullable: false},
					{Name: "created_at", Type: string(types.QValueKindTimestampTZ), Nullable: false},
				},
				PrimaryKeyColumns: []string{"id"},
				NullableEnabled:   true,
			},
		},
		unchangedToastColumnsMap: map[string][]string{destinationTable: {""}},
		peerdbCols:               &protos.PeerDBColumns{},
		rawTableName:             "raw_events",
		mergeBatchId:             1,
	}

	statement, err := generator.generateMergeStmt(context.Background(), nil, destinationTable)

	require.NoError(t, err)
	require.NotContains(t, statement, "NOT NULL")
	require.Contains(t, statement, `CAST(VAR_COLS:"created_at" AS TIMESTAMP_TZ) AS "CREATED_AT"`)
}

func TestGenerateAddColumnSQL(t *testing.T) {
	t.Parallel()

	t.Run("without default", func(t *testing.T) {
		statement, fallback, err := generateAddColumnSQL("public.invoice", "hidden_from_buyer", "BOOLEAN", nil)

		require.NoError(t, err)
		require.Equal(t,
			`ALTER TABLE "PUBLIC"."INVOICE" ADD COLUMN IF NOT EXISTS "HIDDEN_FROM_BUYER" BOOLEAN`, statement)
		require.Empty(t, fallback)
	})

	t.Run("with default", func(t *testing.T) {
		defaultExpr := "false"
		statement, fallback, err := generateAddColumnSQL(
			"public.invoice", "hidden_from_buyer", "BOOLEAN", &defaultExpr,
		)

		require.NoError(t, err)
		require.Equal(t,
			`ALTER TABLE "PUBLIC"."INVOICE" ADD COLUMN "HIDDEN_FROM_BUYER" BOOLEAN DEFAULT false`, statement)
		require.Equal(t,
			`ALTER TABLE "PUBLIC"."INVOICE" ADD COLUMN IF NOT EXISTS "HIDDEN_FROM_BUYER" BOOLEAN`, fallback)
		require.False(t, strings.Contains(statement, "IF NOT EXISTS"))
	})

	t.Run("mixed case identifiers", func(t *testing.T) {
		statement, _, err := generateAddColumnSQL("Analytics.InvoiceEvents", "BuyerID", "STRING", nil)

		require.NoError(t, err)
		require.Equal(t,
			`ALTER TABLE "Analytics"."InvoiceEvents" ADD COLUMN IF NOT EXISTS "BuyerID" STRING`, statement)
	})
}

func TestSnowflakeDefaultExpr(t *testing.T) {
	t.Parallel()

	booleanDefault := "false"
	dateDefault := "'2026-09-02'"
	require.Same(t, &booleanDefault, snowflakeDefaultExpr(types.QValueKindBoolean, &booleanDefault))
	require.Nil(t, snowflakeDefaultExpr(types.QValueKindDate, &dateDefault))
}
