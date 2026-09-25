package peerflow

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/workflows/cdc_state"
)

const RefreshSnowflakeSchemaUpdate = "RefreshSnowflakeSchema"

func validateSnowflakeSchemaRefresh(
	cfg *protos.FlowConnectionConfigsCore,
	state *cdc_state.CDCFlowWorkflowState,
	req *protos.RefreshSnowflakeSchemaRequest,
) error {
	if req == nil || req.FlowJobName != cfg.FlowJobName || req.RequestId == "" || len(req.Tables) == 0 {
		return fmt.Errorf("schema refresh requires the mirror name, request ID, and selected tables")
	}
	if state.CurrentFlowStatus != protos.FlowStatus_STATUS_PAUSED || state.ActiveSignal != model.PauseSignal {
		return fmt.Errorf("schema refresh requires a paused mirror")
	}
	if state.FlowConfigUpdate != nil || state.SchemaRefreshInProgress {
		return fmt.Errorf("another mirror update is already pending")
	}
	return nil
}

func registerSnowflakeSchemaRefresh(
	ctx workflow.Context,
	cfg *protos.FlowConnectionConfigsCore,
	state *cdc_state.CDCFlowWorkflowState,
) error {
	validate := func(req *protos.RefreshSnowflakeSchemaRequest) error {
		return validateSnowflakeSchemaRefresh(cfg, state, req)
	}
	return workflow.SetUpdateHandlerWithOptions(ctx, RefreshSnowflakeSchemaUpdate,
		func(ctx workflow.Context, req *protos.RefreshSnowflakeSchemaRequest) (*protos.RefreshSnowflakeSchemaResponse, error) {
			// Validators can run before another accepted update starts its handler.
			if err := validate(req); err != nil {
				return nil, err
			}
			state.SchemaRefreshInProgress = true
			defer func() { state.SchemaRefreshInProgress = false }()
			activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				ScheduleToCloseTimeout: 5 * time.Minute,
				StartToCloseTimeout:    5 * time.Minute,
				HeartbeatTimeout:       time.Minute,
				WaitForCancellation:    true,
				RetryPolicy: &temporal.RetryPolicy{
					InitialInterval: 5 * time.Second,
					MaximumAttempts: 3,
				},
			})
			var result *protos.RefreshSnowflakeSchemaResponse
			err := workflow.ExecuteActivity(activityCtx, flowable.RefreshSnowflakeSchema,
				updateFlowConfigWithLatestSettings(cfg, state), req).Get(activityCtx, &result)
			return result, err
		}, workflow.UpdateHandlerOptions{Validator: validate})
}

func waitForSnowflakeSchemaRefresh(ctx workflow.Context, state *cdc_state.CDCFlowWorkflowState) error {
	if !state.SchemaRefreshInProgress {
		return nil
	}
	return workflow.Await(ctx, func() bool { return !state.SchemaRefreshInProgress })
}
