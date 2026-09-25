package cmd

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/google/uuid"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	peerflow "github.com/PeerDB-io/peerdb/flow/workflows"
)

func canonicalSchemaRefreshRequest(req *protos.RefreshSnowflakeSchemaRequest) (*protos.RefreshSnowflakeSchemaRequest, error) {
	if req == nil || strings.TrimSpace(req.FlowJobName) == "" || len(req.Tables) == 0 {
		return nil, errors.New("mirror name and explicit table/column removals are required")
	}
	id, err := uuid.Parse(req.RequestId)
	if err != nil || id == uuid.Nil {
		return nil, errors.New("request_id must be a nonzero UUID; reuse it when retrying an uncertain response")
	}
	req = proto.CloneOf(req)
	req.RequestId = id.String()
	seen := make(map[string]bool, len(req.Tables))
	for _, table := range req.Tables {
		if table == nil || strings.TrimSpace(table.SourceTableIdentifier) == "" || len(table.Columns) == 0 {
			return nil, errors.New("each selected source table requires explicit columns")
		}
		if seen[table.SourceTableIdentifier] {
			return nil, fmt.Errorf("duplicate source table %s", table.SourceTableIdentifier)
		}
		seen[table.SourceTableIdentifier] = true
		slices.Sort(table.Columns)
		for i, column := range table.Columns {
			if strings.TrimSpace(column) == "" || (i > 0 && column == table.Columns[i-1]) {
				return nil, fmt.Errorf("empty or duplicate column in %s", table.SourceTableIdentifier)
			}
		}
	}
	slices.SortFunc(req.Tables, func(a, b *protos.SnowflakeSchemaColumnRemoval) int {
		return strings.Compare(a.SourceTableIdentifier, b.SourceTableIdentifier)
	})
	return req, nil
}

func schemaRefreshUpdateError(err error) APIError {
	var activityError *temporal.ActivityError
	var applicationError *temporal.ApplicationError
	if errors.As(err, &activityError) || errors.As(err, &applicationError) ||
		temporal.IsTimeoutError(err) || temporal.IsCanceledError(err) {
		return NewFailedPreconditionApiError(fmt.Errorf(
			"schema refresh update failed; inspect and resolve the cause before using a new request_id: %w", err))
	}
	// A transport/request timeout leaves the update's outcome unknown. It must
	// be retried with the same ID, unlike a confirmed failed workflow update.
	return AsAPIError(err)
}

// RefreshSnowflakeSchema waits for a receipt in the current paused workflow run.
// A disconnected HTTP request does not cancel an accepted update. Exact requests
// repeated after resume/continue-as-new are revalidated idempotently by the activity.
func (h *FlowRequestHandler) RefreshSnowflakeSchema(
	ctx context.Context,
	req *protos.RefreshSnowflakeSchemaRequest,
) (*protos.RefreshSnowflakeSchemaResponse, APIError) {
	req, err := canonicalSchemaRefreshRequest(req)
	if err != nil {
		return nil, NewInvalidArgumentApiError(err)
	}
	if maintenance, err := internal.PeerDBMaintenanceModeEnabled(ctx, nil); err != nil {
		return nil, NewInternalApiError(err)
	} else if maintenance {
		return nil, NewUnavailableApiError(ErrUnderMaintenance)
	}
	workflowID, err := h.getWorkflowID(ctx, req.FlowJobName)
	if err != nil {
		return nil, AsAPIError(err)
	}
	handle, err := h.temporalClient.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   workflowID,
		UpdateID:     "refresh-snowflake-schema/" + req.RequestId,
		UpdateName:   peerflow.RefreshSnowflakeSchemaUpdate,
		Args:         []interface{}{req},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return nil, AsAPIError(err)
	}
	var response protos.RefreshSnowflakeSchemaResponse
	if err := handle.Get(ctx, &response); err != nil {
		return nil, schemaRefreshUpdateError(err)
	}
	// Temporal deduplicates by update ID, not by its arguments. Never return a
	// previous operation's receipt as success for different requested columns.
	if !slices.EqualFunc(req.Tables, response.Tables, func(a, b *protos.SnowflakeSchemaColumnRemoval) bool {
		return proto.Equal(a, b)
	}) || response.NormalizedBatchId < 0 {
		return nil, NewFailedPreconditionApiError(errors.New("schema refresh receipt differs from request; request_id may have been reused"))
	}
	return &response, nil
}
