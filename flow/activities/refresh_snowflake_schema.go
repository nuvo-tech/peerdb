package activities

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	connmetadata "github.com/PeerDB-io/peerdb/flow/connectors/external_metadata"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/concurrency"
)

func invalidSnowflakeSchemaRefresh(format string, args ...any) error {
	return temporal.NewNonRetryableApplicationError(fmt.Sprintf(format, args...), "InvalidSnowflakeSchemaRefresh", nil)
}

func snowflakeSchemaRefreshMappings(
	cfg *protos.FlowConnectionConfigsCore,
	req *protos.RefreshSnowflakeSchemaRequest,
) ([]*protos.TableMapping, error) {
	if cfg == nil || req == nil || req.FlowJobName == "" || req.FlowJobName != cfg.FlowJobName ||
		strings.TrimSpace(req.RequestId) == "" || len(req.Tables) == 0 || cfg.SourceName == cfg.DestinationName {
		return nil, invalidSnowflakeSchemaRefresh("refresh requires the exact mirror, request ID, and selected tables")
	}
	bySource := make(map[string]*protos.TableMapping, len(cfg.TableMappings))
	destinations := make(map[string]bool, len(cfg.TableMappings))
	for _, mapping := range cfg.TableMappings {
		if mapping == nil || mapping.SourceTableIdentifier == "" || mapping.DestinationTableIdentifier == "" ||
			bySource[mapping.SourceTableIdentifier] != nil || destinations[mapping.DestinationTableIdentifier] {
			return nil, invalidSnowflakeSchemaRefresh("refresh requires unique source and destination mappings")
		}
		bySource[mapping.SourceTableIdentifier] = mapping
		destinations[mapping.DestinationTableIdentifier] = true
	}
	selected := make([]*protos.TableMapping, 0, len(req.Tables))
	seenTables := make(map[string]bool, len(req.Tables))
	for _, removal := range req.Tables {
		if removal == nil || seenTables[removal.SourceTableIdentifier] || len(removal.Columns) == 0 {
			return nil, invalidSnowflakeSchemaRefresh("each selected table must be unique and specify columns")
		}
		mapping := bySource[removal.SourceTableIdentifier]
		if mapping == nil {
			return nil, invalidSnowflakeSchemaRefresh("source table %q is not mapped", removal.SourceTableIdentifier)
		}
		seenTables[removal.SourceTableIdentifier] = true
		seenColumns := make(map[string]bool, len(removal.Columns))
		for _, column := range removal.Columns {
			if strings.TrimSpace(column) == "" || seenColumns[column] ||
				strings.EqualFold(column, cfg.SoftDeleteColName) || strings.EqualFold(column, cfg.SyncedAtColName) {
				return nil, invalidSnowflakeSchemaRefresh("invalid, duplicate, or metadata column %q", column)
			}
			seenColumns[column] = true
		}
		selected = append(selected, mapping)
	}
	return selected, nil
}

func validateSnowflakeSchemaRefresh(
	logger log.Logger,
	mappings []*protos.TableMapping,
	req *protos.RefreshSnowflakeSchemaRequest,
	cached, source map[string]*protos.TableSchema,
) (map[string]*protos.TableSchema, error) {
	for i, mapping := range mappings {
		previous, current := cached[mapping.DestinationTableIdentifier], source[mapping.SourceTableIdentifier]
		if previous == nil || current == nil {
			return nil, invalidSnowflakeSchemaRefresh("missing schema for %s", mapping.SourceTableIdentifier)
		}
		for _, column := range req.Tables[i].Columns {
			if slices.Contains(previous.PrimaryKeyColumns, column) || slices.Contains(current.PrimaryKeyColumns, column) {
				return nil, invalidSnowflakeSchemaRefresh("cannot remove primary key column %s.%s", mapping.SourceTableIdentifier, column)
			}
			for _, field := range current.Columns {
				if field.Name == column {
					return nil, invalidSnowflakeSchemaRefresh("column %s.%s still exists on the source", mapping.SourceTableIdentifier, column)
				}
			}
		}
	}
	processed := internal.BuildProcessedSchemaMapping(mappings, source, logger)
	replacements := make(map[string]*protos.TableSchema, len(mappings))
	for i, mapping := range mappings {
		previous := proto.CloneOf(cached[mapping.DestinationTableIdentifier])
		previous.Columns = slices.DeleteFunc(previous.Columns, func(field *protos.FieldDescription) bool {
			return slices.Contains(req.Tables[i].Columns, field.Name)
		})
		current := proto.CloneOf(processed[mapping.DestinationTableIdentifier])
		if len(previous.Columns) == len(current.Columns) {
			for j, field := range previous.Columns {
				// CDC can retain nullable metadata after SET NOT NULL on the source.
				// Keep that permissive cached metadata; this operation only removes columns.
				if field.Name == current.Columns[j].Name && field.Nullable {
					current.Columns[j].Nullable = true
				}
			}
		}
		if !proto.Equal(previous, current) {
			return nil, invalidSnowflakeSchemaRefresh("schema for %s has drift beyond the selected removed columns", mapping.SourceTableIdentifier)
		}
		replacements[mapping.DestinationTableIdentifier] = previous
	}
	return replacements, nil
}

type snowflakeSchemaRefreshDeps struct {
	loadCatalog func(context.Context) (map[string]*protos.TableSchema, error)
	loadSource  func(context.Context) (map[string]*protos.TableSchema, error)
	progress    func(context.Context) (int64, int64, error)
	normalize   func(context.Context, int64) error
	update      func(context.Context, func(map[string]*protos.TableSchema) (map[string]*protos.TableSchema, error)) error
}

func refreshSnowflakeSchema(
	ctx context.Context,
	logger log.Logger,
	mappings []*protos.TableMapping,
	req *protos.RefreshSnowflakeSchemaRequest,
	deps snowflakeSchemaRefreshDeps,
) (*protos.RefreshSnowflakeSchemaResponse, error) {
	cached, err := deps.loadCatalog(ctx)
	if err != nil {
		return nil, err
	}
	source, err := deps.loadSource(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := validateSnowflakeSchemaRefresh(logger, mappings, req, cached, source); err != nil {
		return nil, err
	}
	boundary, normalized, err := deps.progress(ctx)
	if err != nil {
		return nil, err
	}
	// Keep the old schema until every committed raw batch has normalized.
	if normalized < boundary {
		if err := deps.normalize(ctx, boundary); err != nil {
			return nil, fmt.Errorf("failed to drain Snowflake normalization before schema refresh: %w", err)
		}
	}
	var replacements map[string]*protos.TableSchema
	err = deps.update(ctx, func(current map[string]*protos.TableSchema) (map[string]*protos.TableSchema, error) {
		freshSource, err := deps.loadSource(ctx)
		if err != nil {
			return nil, err
		}
		replacements, err = validateSnowflakeSchemaRefresh(logger, mappings, req, current, freshSource)
		if err != nil {
			return nil, err
		}
		synced, drained, err := deps.progress(ctx)
		if err != nil {
			return nil, err
		}
		if synced != boundary || drained < boundary {
			return nil, invalidSnowflakeSchemaRefresh("mirror progressed or normalization did not drain at batch %d", boundary)
		}
		normalized = drained
		return replacements, nil
	})
	if err != nil {
		return nil, err
	}
	readback, err := deps.loadCatalog(ctx)
	if err != nil {
		return nil, err
	}
	for destination, expected := range replacements {
		if !proto.Equal(expected, readback[destination]) {
			return nil, invalidSnowflakeSchemaRefresh("catalog readback differs for %s; keep mirror paused", destination)
		}
	}
	return &protos.RefreshSnowflakeSchemaResponse{Tables: req.Tables, NormalizedBatchId: normalized}, nil
}

// RefreshSnowflakeSchema is called only by the paused workflow, which holds all
// state changes until this activity finishes. It changes catalog schemas only.
func (a *FlowableActivity) RefreshSnowflakeSchema(
	ctx context.Context,
	cfg *protos.FlowConnectionConfigsCore,
	req *protos.RefreshSnowflakeSchemaRequest,
) (*protos.RefreshSnowflakeSchemaResponse, error) {
	mappings, err := snowflakeSchemaRefreshMappings(cfg, req)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, shared.FlowNameKey, cfg.FlowJobName)
	logger := internal.LoggerFromCtx(ctx)
	shutdown := common.HeartbeatRoutine(ctx, func() string { return "refreshing selected Snowflake catalog columns" })
	defer shutdown()
	for peer, expected := range map[string]protos.DBType{cfg.SourceName: protos.DBType_POSTGRES, cfg.DestinationName: protos.DBType_SNOWFLAKE} {
		peerType, err := connectors.LoadPeerType(ctx, a.CatalogPool, peer)
		if err != nil {
			return nil, err
		}
		if peerType != expected {
			return nil, invalidSnowflakeSchemaRefresh("refresh requires a PostgreSQL source and Snowflake destination")
		}
	}
	source, closeSource, err := connectors.GetByNameAs[connectors.GetTableSchemaConnector](ctx, cfg.Env, a.CatalogPool, cfg.SourceName)
	if err != nil {
		return nil, err
	}
	defer closeSource(ctx)
	uncensored := make([]*protos.TableMapping, 0, len(mappings))
	destinations := make([]string, 0, len(mappings))
	for _, mapping := range mappings {
		copy := proto.CloneOf(mapping)
		copy.Exclude = nil
		uncensored = append(uncensored, copy)
		destinations = append(destinations, mapping.DestinationTableIdentifier)
	}
	metadata := connmetadata.NewPostgresMetadataFromCatalog(logger, a.CatalogPool)
	return refreshSnowflakeSchema(ctx, logger, mappings, req, snowflakeSchemaRefreshDeps{
		loadCatalog: func(ctx context.Context) (map[string]*protos.TableSchema, error) {
			return internal.LoadTableSchemasFromCatalog(ctx, a.CatalogPool, cfg.FlowJobName, destinations)
		},
		loadSource: func(ctx context.Context) (map[string]*protos.TableSchema, error) {
			return source.GetTableSchema(ctx, cfg.Env, cfg.Version, cfg.System, uncensored)
		},
		progress: func(ctx context.Context) (int64, int64, error) {
			synced, err := metadata.GetLastSyncBatchID(ctx, cfg.FlowJobName)
			if err != nil {
				return 0, 0, err
			}
			normalized, err := metadata.GetLastNormalizeBatchID(ctx, cfg.FlowJobName)
			return synced, normalized, err
		},
		normalize: func(ctx context.Context, boundary int64) error {
			responses := concurrency.NewLastChan()
			defer responses.Close()
			return a.startNormalize(ctx, cfg, boundary, responses)
		},
		update: func(ctx context.Context, modify func(map[string]*protos.TableSchema) (map[string]*protos.TableSchema, error)) error {
			return internal.ReadModifyWriteTableSchemasToCatalog(ctx, a.CatalogPool, logger, cfg.FlowJobName, destinations, modify)
		},
	})
}
