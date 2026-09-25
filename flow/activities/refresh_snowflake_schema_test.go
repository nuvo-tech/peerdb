package activities

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func snowflakeRefreshFixture() (
	*protos.FlowConnectionConfigsCore, *protos.RefreshSnowflakeSchemaRequest,
	map[string]*protos.TableSchema, map[string]*protos.TableSchema,
) {
	cfg := &protos.FlowConnectionConfigsCore{
		FlowJobName: "app", SourceName: "postgres", DestinationName: "snowflake",
		SoftDeleteColName: "_peerdb_is_deleted", SyncedAtColName: "_peerdb_synced_at",
		TableMappings: []*protos.TableMapping{{SourceTableIdentifier: "public.user", DestinationTableIdentifier: "raw.user"}},
	}
	req := &protos.RefreshSnowflakeSchemaRequest{
		FlowJobName: "app", RequestId: "refresh-password",
		Tables: []*protos.SnowflakeSchemaColumnRemoval{{SourceTableIdentifier: "public.user", Columns: []string{"password"}}},
	}
	old := &protos.TableSchema{
		TableIdentifier: "public.user", TableOid: 42, PrimaryKeyColumns: []string{"id"},
		Columns: []*protos.FieldDescription{{Name: "id", Type: "int64"}, {Name: "password", Type: "string"}, {Name: "name", Type: "string"}},
	}
	current := proto.CloneOf(old)
	current.Columns = slices.Delete(current.Columns, 1, 2)
	return cfg, req, map[string]*protos.TableSchema{"raw.user": old}, map[string]*protos.TableSchema{"public.user": current}
}

func requireInvalidRefresh(t *testing.T, err error) {
	t.Helper()
	var applicationError *temporal.ApplicationError
	require.ErrorAs(t, err, &applicationError)
	require.True(t, applicationError.NonRetryable())
}

func TestSnowflakeSchemaRefreshMappings(t *testing.T) {
	tests := map[string]func(*protos.FlowConnectionConfigsCore, *protos.RefreshSnowflakeSchemaRequest){
		"wrong mirror": func(_ *protos.FlowConnectionConfigsCore, req *protos.RefreshSnowflakeSchemaRequest) {
			req.FlowJobName = "other"
		},
		"missing request ID": func(_ *protos.FlowConnectionConfigsCore, req *protos.RefreshSnowflakeSchemaRequest) {
			req.RequestId = " "
		},
		"unknown table": func(_ *protos.FlowConnectionConfigsCore, req *protos.RefreshSnowflakeSchemaRequest) {
			req.Tables[0].SourceTableIdentifier = "public.other"
		},
		"duplicate table": func(_ *protos.FlowConnectionConfigsCore, req *protos.RefreshSnowflakeSchemaRequest) {
			req.Tables = append(req.Tables, proto.CloneOf(req.Tables[0]))
		},
		"no columns": func(_ *protos.FlowConnectionConfigsCore, req *protos.RefreshSnowflakeSchemaRequest) {
			req.Tables[0].Columns = nil
		},
		"empty column": func(_ *protos.FlowConnectionConfigsCore, req *protos.RefreshSnowflakeSchemaRequest) {
			req.Tables[0].Columns = []string{" "}
		},
		"duplicate column": func(_ *protos.FlowConnectionConfigsCore, req *protos.RefreshSnowflakeSchemaRequest) {
			req.Tables[0].Columns = []string{"password", "password"}
		},
		"soft delete column": func(_ *protos.FlowConnectionConfigsCore, req *protos.RefreshSnowflakeSchemaRequest) {
			req.Tables[0].Columns = []string{"_PEERDB_IS_DELETED"}
		},
		"sync timestamp": func(_ *protos.FlowConnectionConfigsCore, req *protos.RefreshSnowflakeSchemaRequest) {
			req.Tables[0].Columns = []string{"_peerdb_synced_at"}
		},
		"shared destination": func(cfg *protos.FlowConnectionConfigsCore, _ *protos.RefreshSnowflakeSchemaRequest) {
			cfg.TableMappings = append(cfg.TableMappings, &protos.TableMapping{SourceTableIdentifier: "public.other", DestinationTableIdentifier: "raw.user"})
		},
	}
	for name, change := range tests {
		t.Run(name, func(t *testing.T) {
			cfg, req, _, _ := snowflakeRefreshFixture()
			change(cfg, req)
			_, err := snowflakeSchemaRefreshMappings(cfg, req)
			requireInvalidRefresh(t, err)
		})
	}
}

func TestValidateSnowflakeSchemaRefresh(t *testing.T) {
	type change func(*protos.FlowConnectionConfigsCore, *protos.RefreshSnowflakeSchemaRequest, map[string]*protos.TableSchema, map[string]*protos.TableSchema)
	tests := map[string]change{
		"column still present": func(_ *protos.FlowConnectionConfigsCore, _ *protos.RefreshSnowflakeSchemaRequest, cached, source map[string]*protos.TableSchema) {
			source["public.user"] = proto.CloneOf(cached["raw.user"])
		},
		"excluded column still present": func(cfg *protos.FlowConnectionConfigsCore, _ *protos.RefreshSnowflakeSchemaRequest, cached, source map[string]*protos.TableSchema) {
			cfg.TableMappings[0].Exclude = []string{"password"}
			source["public.user"] = proto.CloneOf(cached["raw.user"])
		},
		"primary key": func(_ *protos.FlowConnectionConfigsCore, req *protos.RefreshSnowflakeSchemaRequest, _ map[string]*protos.TableSchema, _ map[string]*protos.TableSchema) {
			req.Tables[0].Columns = []string{"id", "password"}
		},
		"type change": func(_ *protos.FlowConnectionConfigsCore, _ *protos.RefreshSnowflakeSchemaRequest, _ map[string]*protos.TableSchema, source map[string]*protos.TableSchema) {
			source["public.user"].Columns[1].Type = "int64"
		},
		"nullability change": func(_ *protos.FlowConnectionConfigsCore, _ *protos.RefreshSnowflakeSchemaRequest, _ map[string]*protos.TableSchema, source map[string]*protos.TableSchema) {
			source["public.user"].Columns[1].Nullable = true
		},
		"added column": func(_ *protos.FlowConnectionConfigsCore, _ *protos.RefreshSnowflakeSchemaRequest, _ map[string]*protos.TableSchema, source map[string]*protos.TableSchema) {
			source["public.user"].Columns = append(source["public.user"].Columns, &protos.FieldDescription{Name: "new", Type: "string"})
		},
		"unselected removed column": func(_ *protos.FlowConnectionConfigsCore, _ *protos.RefreshSnowflakeSchemaRequest, _ map[string]*protos.TableSchema, source map[string]*protos.TableSchema) {
			source["public.user"].Columns = source["public.user"].Columns[:1]
		},
		"recreated source table": func(_ *protos.FlowConnectionConfigsCore, _ *protos.RefreshSnowflakeSchemaRequest, _ map[string]*protos.TableSchema, source map[string]*protos.TableSchema) {
			source["public.user"].TableOid++
		},
		"missing catalog schema": func(_ *protos.FlowConnectionConfigsCore, _ *protos.RefreshSnowflakeSchemaRequest, cached map[string]*protos.TableSchema, _ map[string]*protos.TableSchema) {
			delete(cached, "raw.user")
		},
	}
	for name, change := range tests {
		t.Run(name, func(t *testing.T) {
			cfg, req, cached, source := snowflakeRefreshFixture()
			change(cfg, req, cached, source)
			_, err := validateSnowflakeSchemaRefresh(log.NewStructuredLogger(slog.Default()), cfg.TableMappings, req, cached, source)
			requireInvalidRefresh(t, err)
		})
	}

	t.Run("removes only selected columns and is idempotent", func(t *testing.T) {
		cfg, req, cached, source := snowflakeRefreshFixture()
		original := proto.CloneOf(cached["raw.user"])
		cached["raw.other"] = &protos.TableSchema{TableIdentifier: "public.other"}
		logger := log.NewStructuredLogger(slog.Default())
		result, err := validateSnowflakeSchemaRefresh(logger, cfg.TableMappings, req, cached, source)
		require.NoError(t, err)
		require.Len(t, result, 1)
		require.True(t, proto.Equal(source["public.user"], result["raw.user"]))
		require.True(t, proto.Equal(original, cached["raw.user"]), "validation must not mutate cached schema")
		second, err := validateSnowflakeSchemaRefresh(logger, cfg.TableMappings, req, result, source)
		require.NoError(t, err)
		require.True(t, proto.Equal(result["raw.user"], second["raw.user"]))
	})

	t.Run("preserves existing exclusions", func(t *testing.T) {
		cfg, req, cached, source := snowflakeRefreshFixture()
		cfg.TableMappings[0].Exclude = []string{"private_note"}
		source["public.user"].Columns = append(source["public.user"].Columns, &protos.FieldDescription{Name: "private_note", Type: "string"})
		result, err := validateSnowflakeSchemaRefresh(log.NewStructuredLogger(slog.Default()), cfg.TableMappings, req, cached, source)
		require.NoError(t, err)
		require.Len(t, result["raw.user"].Columns, 2)
		require.Equal(t, []string{"private_note"}, cfg.TableMappings[0].Exclude)
	})
}

func TestRefreshSnowflakeSchemaDrainAndRetry(t *testing.T) {
	cfg, req, cached, source := snowflakeRefreshFixture()
	logger := log.NewStructuredLogger(slog.Default())
	normalized := int64(5)
	normalizeCalls := 0
	deps := snowflakeSchemaRefreshDeps{
		loadCatalog: func(context.Context) (map[string]*protos.TableSchema, error) { return cached, nil },
		loadSource:  func(context.Context) (map[string]*protos.TableSchema, error) { return source, nil },
		progress:    func(context.Context) (int64, int64, error) { return 6, normalized, nil },
		normalize: func(_ context.Context, boundary int64) error {
			require.Len(t, cached["raw.user"].Columns, 3, "pending rows must use the old schema")
			require.Equal(t, int64(6), boundary)
			normalizeCalls++
			normalized = boundary
			return nil
		},
		update: func(_ context.Context, modify func(map[string]*protos.TableSchema) (map[string]*protos.TableSchema, error)) error {
			replacements, err := modify(cached)
			if err == nil {
				for name, schema := range replacements {
					cached[name] = schema
				}
			}
			return err
		},
	}
	for range 2 {
		result, err := refreshSnowflakeSchema(t.Context(), logger, cfg.TableMappings, req, deps)
		require.NoError(t, err)
		require.Equal(t, int64(6), result.NormalizedBatchId)
		require.True(t, proto.Equal(source["public.user"], cached["raw.user"]))
	}
	require.Equal(t, 1, normalizeCalls, "retry after catalog commit must not repeat normalization")
}

func TestRefreshSnowflakeSchemaRejectsUnsafeCommit(t *testing.T) {
	for _, scenario := range []string{"normalization fails", "normalization incomplete", "sync advanced", "source changed", "catalog changed"} {
		t.Run(scenario, func(t *testing.T) {
			cfg, req, cached, source := snowflakeRefreshFixture()
			original := proto.CloneOf(cached["raw.user"])
			synced, normalized := int64(6), int64(5)
			committed := false
			sourceReads := 0
			deps := snowflakeSchemaRefreshDeps{
				loadCatalog: func(context.Context) (map[string]*protos.TableSchema, error) { return cached, nil },
				loadSource: func(context.Context) (map[string]*protos.TableSchema, error) {
					sourceReads++
					if scenario == "source changed" && sourceReads > 1 {
						source["public.user"] = proto.CloneOf(original)
					}
					return source, nil
				},
				progress: func(context.Context) (int64, int64, error) { return synced, normalized, nil },
				normalize: func(_ context.Context, boundary int64) error {
					if scenario == "normalization fails" {
						return errors.New("MERGE failed")
					}
					if scenario != "normalization incomplete" {
						normalized = boundary
					}
					if scenario == "sync advanced" {
						synced++
					}
					return nil
				},
				update: func(_ context.Context, modify func(map[string]*protos.TableSchema) (map[string]*protos.TableSchema, error)) error {
					current := map[string]*protos.TableSchema{"raw.user": proto.CloneOf(cached["raw.user"])}
					if scenario == "catalog changed" {
						current["raw.user"].Columns[2].Type = "int64"
					}
					_, err := modify(current)
					committed = err == nil
					return err
				},
			}
			_, err := refreshSnowflakeSchema(t.Context(), log.NewStructuredLogger(slog.Default()), cfg.TableMappings, req, deps)
			require.Error(t, err)
			require.False(t, committed)
			require.True(t, proto.Equal(original, cached["raw.user"]))
		})
	}
}
