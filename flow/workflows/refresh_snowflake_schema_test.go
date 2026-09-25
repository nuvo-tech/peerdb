package peerflow

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/workflows/cdc_state"
)

func schemaRefreshTestInput() (*protos.FlowConnectionConfigsCore, *cdc_state.CDCFlowWorkflowState, *protos.RefreshSnowflakeSchemaRequest) {
	cfg := &protos.FlowConnectionConfigsCore{FlowJobName: "schema-refresh"}
	state := &cdc_state.CDCFlowWorkflowState{
		CurrentFlowStatus: protos.FlowStatus_STATUS_PAUSED,
		ActiveSignal:      model.PauseSignal,
		SyncFlowOptions:   &protos.SyncFlowOptions{},
	}
	req := &protos.RefreshSnowflakeSchemaRequest{
		FlowJobName: cfg.FlowJobName,
		RequestId:   "remove-password",
		Tables: []*protos.SnowflakeSchemaColumnRemoval{{
			SourceTableIdentifier: "public.user",
			Columns:               []string{"password_hash"},
		}},
	}
	return cfg, state, req
}

func schemaRefreshTestEnvironment(t *testing.T) (*testsuite.TestWorkflowEnvironment, *atomic.Int32) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivityWithOptions(
		func(_ context.Context, _ string, status protos.FlowStatus) (protos.FlowStatus, error) {
			return status, nil
		}, activity.RegisterOptions{Name: "updateFlowStatusInCatalogActivity"})
	var configWrites atomic.Int32
	env.RegisterActivityWithOptions(
		func(context.Context, *protos.FlowConnectionConfigsCore) error {
			configWrites.Add(1)
			return nil
		}, activity.RegisterOptions{Name: "UpdateCDCConfigInCatalogActivity"})
	return env, &configWrites
}

func querySchemaRefreshState(t *testing.T, env *testsuite.TestWorkflowEnvironment) cdc_state.CDCFlowWorkflowState {
	t.Helper()
	value, err := env.QueryWorkflow(shared.CDCFlowStateQuery)
	require.NoError(t, err)
	var state cdc_state.CDCFlowWorkflowState
	require.NoError(t, value.Get(&state))
	return state
}

func TestSchemaRefreshSerializesPausedTransitions(t *testing.T) {
	for _, transition := range []string{"resume", "config", "terminate", "resync"} {
		t.Run(transition, func(t *testing.T) {
			env, configWrites := schemaRefreshTestEnvironment(t)
			cfg, state, req := schemaRefreshTestInput()
			response := &protos.RefreshSnowflakeSchemaResponse{Tables: req.Tables, NormalizedBatchId: 42}
			env.OnActivity(flowable.RefreshSnowflakeSchema, mock.Anything, mock.Anything, mock.Anything).
				Return(response, nil).After(30 * time.Second).Once()
			completed := false
			env.RegisterDelayedCallback(func() {
				env.UpdateWorkflow(RefreshSnowflakeSchemaUpdate, req.RequestId, &testsuite.TestUpdateCallback{
					OnReject: func(err error) { require.NoError(t, err) },
					OnComplete: func(value any, err error) {
						require.NoError(t, err)
						require.Equal(t, int64(42), value.(*protos.RefreshSnowflakeSchemaResponse).NormalizedBatchId)
						completed = true
					},
				}, req)
			}, 0)
			env.RegisterDelayedCallback(func() {
				switch transition {
				case "resume":
					env.SignalWorkflow(model.FlowSignal.Name, model.NoopSignal)
				case "config":
					env.SignalWorkflow(model.CDCDynamicPropertiesSignal.Name, &protos.CDCFlowConfigUpdate{BatchSize: 99})
				case "terminate":
					env.SignalWorkflow(model.FlowSignalStateChange.Name,
						&protos.FlowStateChangeRequest{RequestedFlowState: protos.FlowStatus_STATUS_TERMINATING})
				case "resync":
					env.SignalWorkflow(model.FlowSignalStateChange.Name,
						&protos.FlowStateChangeRequest{RequestedFlowState: protos.FlowStatus_STATUS_RESYNC})
				}
			}, time.Second)
			env.RegisterDelayedCallback(func() {
				current := querySchemaRefreshState(t, env)
				require.True(t, current.SchemaRefreshInProgress)
				require.Equal(t, protos.FlowStatus_STATUS_PAUSED, current.CurrentFlowStatus)
				require.Nil(t, current.DropFlowInput)
				require.Zero(t, current.SyncFlowOptions.BatchSize)
				require.Zero(t, configWrites.Load())
				require.False(t, completed)
				second := &protos.RefreshSnowflakeSchemaRequest{
					FlowJobName: req.FlowJobName, RequestId: "second", Tables: req.Tables,
				}
				env.UpdateWorkflow(RefreshSnowflakeSchemaUpdate, second.RequestId, &testsuite.TestUpdateCallback{
					OnAccept: func() { t.Error("concurrent refresh was accepted") },
					OnReject: func(err error) { require.Error(t, err) },
				}, second)
			}, 2*time.Second)
			env.ExecuteWorkflow(CDCFlowWorkflow, cfg, state)
			require.True(t, completed)
			var continued *workflow.ContinueAsNewError
			require.ErrorAs(t, env.GetWorkflowError(), &continued)
			if transition == "resume" {
				require.Zero(t, configWrites.Load())
			} else {
				require.Positive(t, configWrites.Load())
			}
			env.AssertExpectations(t)
		})
	}
}

func TestSchemaRefreshKeepsMirrorPaused(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "success"
		if fail {
			name = "failure"
		}
		t.Run(name, func(t *testing.T) {
			env, configWrites := schemaRefreshTestEnvironment(t)
			cfg, state, req := schemaRefreshTestInput()
			var response *protos.RefreshSnowflakeSchemaResponse
			var activityErr error
			if fail {
				activityErr = temporal.NewNonRetryableApplicationError("source schema changed", "invalid_schema", nil)
			} else {
				response = &protos.RefreshSnowflakeSchemaResponse{Tables: req.Tables, NormalizedBatchId: 42}
			}
			env.OnActivity(flowable.RefreshSnowflakeSchema, mock.Anything, mock.Anything, mock.Anything).
				Return(response, activityErr).After(time.Second).Once()
			completed := false
			env.RegisterDelayedCallback(func() {
				env.UpdateWorkflow(RefreshSnowflakeSchemaUpdate, req.RequestId, &testsuite.TestUpdateCallback{
					OnReject: func(err error) { require.NoError(t, err) },
					OnComplete: func(_ any, err error) {
						if fail {
							require.Error(t, err)
						} else {
							require.NoError(t, err)
						}
						completed = true
					},
				}, req)
			}, 0)
			env.RegisterDelayedCallback(func() {
				require.True(t, completed)
				current := querySchemaRefreshState(t, env)
				require.False(t, current.SchemaRefreshInProgress)
				require.Equal(t, protos.FlowStatus_STATUS_PAUSED, current.CurrentFlowStatus)
				require.Equal(t, model.PauseSignal, current.ActiveSignal)
				require.Zero(t, configWrites.Load())
				env.CancelWorkflow()
			}, 2*time.Second)
			env.ExecuteWorkflow(CDCFlowWorkflow, cfg, state)
			require.True(t, completed)
			env.AssertExpectations(t)
		})
	}
}

func TestValidateSnowflakeSchemaRefresh(t *testing.T) {
	for _, invalid := range []string{"running", "resuming", "config", "refresh", "wrong mirror", "no request ID", "no tables"} {
		t.Run(invalid, func(t *testing.T) {
			cfg, state, req := schemaRefreshTestInput()
			require.NoError(t, validateSnowflakeSchemaRefresh(cfg, state, req))
			switch invalid {
			case "running":
				state.CurrentFlowStatus = protos.FlowStatus_STATUS_RUNNING
			case "resuming":
				state.ActiveSignal = model.NoopSignal
			case "config":
				state.FlowConfigUpdate = &protos.CDCFlowConfigUpdate{}
			case "refresh":
				state.SchemaRefreshInProgress = true
			case "wrong mirror":
				req.FlowJobName = "another"
			case "no request ID":
				req.RequestId = ""
			case "no tables":
				req.Tables = nil
			}
			require.Error(t, validateSnowflakeSchemaRefresh(cfg, state, req))
		})
	}
}

func TestSchemaRefreshRejectsConcurrentUpdates(t *testing.T) {
	env, _ := schemaRefreshTestEnvironment(t)
	cfg, state, req := schemaRefreshTestInput()
	env.OnActivity(flowable.RefreshSnowflakeSchema, mock.Anything, mock.Anything, mock.Anything).
		Return(&protos.RefreshSnowflakeSchemaResponse{Tables: req.Tables}, nil).After(30 * time.Second).Once()
	completed, rejected := 0, 0
	env.RegisterDelayedCallback(func() {
		for _, id := range []string{"first", "second"} {
			request := &protos.RefreshSnowflakeSchemaRequest{
				FlowJobName: req.FlowJobName, RequestId: id, Tables: req.Tables,
			}
			env.UpdateWorkflow(RefreshSnowflakeSchemaUpdate, id, &testsuite.TestUpdateCallback{
				OnReject: func(err error) {
					require.ErrorContains(t, err, "already pending")
					rejected++
				},
				OnComplete: func(_ any, err error) {
					if err != nil {
						require.ErrorContains(t, err, "already pending")
						rejected++
					} else {
						completed++
					}
				},
			}, request)
		}
	}, 0)
	env.RegisterDelayedCallback(func() {
		require.Equal(t, 1, completed)
		require.Equal(t, 1, rejected)
		env.CancelWorkflow()
	}, 31*time.Second)
	env.ExecuteWorkflow(CDCFlowWorkflow, cfg, state)
	env.AssertExpectations(t)
}

func TestSchemaRefreshCancellationCompletesHandler(t *testing.T) {
	env, _ := schemaRefreshTestEnvironment(t)
	cfg, state, req := schemaRefreshTestInput()
	env.OnActivity(flowable.RefreshSnowflakeSchema, mock.Anything, mock.Anything, mock.Anything).
		Return(&protos.RefreshSnowflakeSchemaResponse{Tables: req.Tables}, nil).After(30 * time.Second).Once()
	completed := false
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(RefreshSnowflakeSchemaUpdate, req.RequestId, &testsuite.TestUpdateCallback{
			OnReject: func(err error) { require.NoError(t, err) },
			OnComplete: func(_ any, err error) {
				require.Error(t, err)
				completed = true
			},
		}, req)
	}, 0)
	env.RegisterDelayedCallback(func() { env.CancelWorkflow() }, time.Second)
	env.ExecuteWorkflow(CDCFlowWorkflow, cfg, state)
	require.True(t, completed, "workflow must not exit with an unfinished refresh handler")
	require.Error(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}
