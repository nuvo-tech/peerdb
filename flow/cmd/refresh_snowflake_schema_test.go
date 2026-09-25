package cmd

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestCanonicalSchemaRefreshRequest(t *testing.T) {
	valid := &protos.RefreshSnowflakeSchemaRequest{
		FlowJobName: "mirror",
		RequestId:   "11111111-1111-4111-8111-111111111111",
		Tables: []*protos.SnowflakeSchemaColumnRemoval{
			{SourceTableIdentifier: "public.user", Columns: []string{"should_reset_password", "password_hash"}},
			{SourceTableIdentifier: "public.company", Columns: []string{"password_hash"}},
		},
	}
	original := proto.CloneOf(valid)
	canonical, err := canonicalSchemaRefreshRequest(valid)
	require.NoError(t, err)
	require.True(t, proto.Equal(original, valid), "canonicalization must not mutate caller data")
	require.Equal(t, "public.company", canonical.Tables[0].SourceTableIdentifier)
	require.Equal(t, []string{"password_hash", "should_reset_password"}, canonical.Tables[1].Columns)

	for _, tc := range []struct {
		name   string
		mutate func(*protos.RefreshSnowflakeSchemaRequest)
	}{
		{"missing mirror", func(r *protos.RefreshSnowflakeSchemaRequest) { r.FlowJobName = "" }},
		{"invalid id", func(r *protos.RefreshSnowflakeSchemaRequest) { r.RequestId = "retry" }},
		{"nil id", func(r *protos.RefreshSnowflakeSchemaRequest) { r.RequestId = "00000000-0000-0000-0000-000000000000" }},
		{"no tables", func(r *protos.RefreshSnowflakeSchemaRequest) { r.Tables = nil }},
		{"nil table", func(r *protos.RefreshSnowflakeSchemaRequest) { r.Tables[0] = nil }},
		{"empty table", func(r *protos.RefreshSnowflakeSchemaRequest) { r.Tables[0].SourceTableIdentifier = " " }},
		{"duplicate table", func(r *protos.RefreshSnowflakeSchemaRequest) {
			r.Tables[1].SourceTableIdentifier = r.Tables[0].SourceTableIdentifier
		}},
		{"no columns", func(r *protos.RefreshSnowflakeSchemaRequest) { r.Tables[0].Columns = nil }},
		{"empty column", func(r *protos.RefreshSnowflakeSchemaRequest) { r.Tables[0].Columns[0] = " " }},
		{"duplicate column", func(r *protos.RefreshSnowflakeSchemaRequest) { r.Tables[0].Columns = []string{"a", "a"} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := proto.CloneOf(valid)
			tc.mutate(request)
			_, err := canonicalSchemaRefreshRequest(request)
			require.Error(t, err)
		})
	}
	_, err = canonicalSchemaRefreshRequest(nil)
	require.Error(t, err)
}

func TestSchemaRefreshUpdateError(t *testing.T) {
	for _, err := range []error{
		temporal.NewNonRetryableApplicationError("source column still exists", "InvalidSnowflakeSchemaRefresh", nil),
		temporal.NewTimeoutError(enums.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE, nil),
		temporal.NewCanceledError("workflow canceled"),
	} {
		apiError := schemaRefreshUpdateError(err)
		require.Equal(t, codes.FailedPrecondition, apiError.GRPCStatus().Code())
		require.Contains(t, apiError.Error(), "new request_id")
	}
	uncertain := schemaRefreshUpdateError(client.NewWorkflowUpdateServiceTimeoutOrCanceledError(context.DeadlineExceeded))
	require.NotEqual(t, codes.FailedPrecondition, uncertain.GRPCStatus().Code())
}
