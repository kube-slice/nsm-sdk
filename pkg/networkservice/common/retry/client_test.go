// Copyright (c) 2021 Cisco and/or its affiliates.
//
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package retry_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/networkservicemesh/api/pkg/api/networkservice"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc"

	"github.com/networkservicemesh/sdk/pkg/networkservice/core/chain"
	"github.com/networkservicemesh/sdk/pkg/networkservice/core/next"
	"github.com/networkservicemesh/sdk/pkg/networkservice/utils/checks/checkcontext"
	"github.com/networkservicemesh/sdk/pkg/networkservice/utils/count"
	"github.com/networkservicemesh/sdk/pkg/tools/clock"
	"github.com/networkservicemesh/sdk/pkg/tools/clockmock"

	"github.com/networkservicemesh/sdk/pkg/networkservice/common/retry"
)

type remoteSideClient struct {
	delay            time.Duration
	failRequestCount int32
	failCloseCount   int32
}

func (c *remoteSideClient) Request(ctx context.Context, request *networkservice.NetworkServiceRequest, opts ...grpc.CallOption) (*networkservice.Connection, error) {
	if atomic.AddInt32(&c.failRequestCount, -1) == -1 {
		return next.Client(ctx).Request(ctx, request, opts...)
	}

	clock.FromContext(ctx).Sleep(c.delay)
	return nil, errors.New("cannot connect")
}

func (c *remoteSideClient) Close(ctx context.Context, conn *networkservice.Connection, opts ...grpc.CallOption) (*empty.Empty, error) {
	if atomic.AddInt32(&c.failCloseCount, -1) == -1 {
		return next.Client(ctx).Close(ctx, conn, opts...)
	}

	clock.FromContext(ctx).Sleep(c.delay)
	return nil, errors.New("cannot connect")
}

func Test_RetryClient_Request(t *testing.T) {
	t.Cleanup(func() { goleak.VerifyNone(t) })

	var counter = new(count.Client)

	var client = retry.NewClient(
		chain.NewNetworkServiceClient(
			counter,
			&remoteSideClient{
				delay:            time.Millisecond * 10,
				failRequestCount: 5,
			},
		),
		nil,
		retry.WithInterval(time.Millisecond*10),
		retry.WithTryTimeout(time.Second/30),
	)

	var _, err = client.Request(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, 6, counter.Requests())
	require.Equal(t, 0, counter.Closes())
}

func Test_RetryClient_Request_ContextHasCorrectDeadline(t *testing.T) {
	t.Cleanup(func() { goleak.VerifyNone(t) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clockMock := clockmock.New(ctx)
	clockMock.SetSpeed(0)

	ctx = clock.WithClock(ctx, clockMock)

	expectedDeadline := clockMock.Now().Add(time.Hour)

	var client = retry.NewClient(chain.NewNetworkServiceClient(
		checkcontext.NewClient(t, func(t *testing.T, c context.Context) {
			v, ok := c.Deadline()
			require.True(t, ok)
			require.Equal(t, expectedDeadline, v)
		}),
	), nil, retry.WithTryTimeout(time.Hour))

	var _, err = client.Request(ctx, nil)
	require.NoError(t, err)
}

func Test_RetryClient_Close_ContextHasCorrectDeadline(t *testing.T) {
	t.Cleanup(func() { goleak.VerifyNone(t) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clockMock := clockmock.New(ctx)
	clockMock.SetSpeed(0)

	ctx = clock.WithClock(ctx, clockMock)

	expectedDeadline := clockMock.Now().Add(time.Hour)

	var client = retry.NewClient(chain.NewNetworkServiceClient(
		checkcontext.NewClient(t, func(t *testing.T, c context.Context) {
			v, ok := c.Deadline()
			require.True(t, ok)
			require.Equal(t, expectedDeadline, v)
		}),
	), nil, retry.WithTryTimeout(time.Hour))

	var _, err = client.Close(ctx, nil)
	require.NoError(t, err)
}

func Test_RetryClient_Close(t *testing.T) {
	t.Cleanup(func() { goleak.VerifyNone(t) })

	var counter = new(count.Client)

	var client = retry.NewClient(
		chain.NewNetworkServiceClient(
			counter,
			&remoteSideClient{
				delay:          time.Millisecond * 10,
				failCloseCount: 5,
			},
		),
		nil,
		retry.WithInterval(time.Millisecond*10),
		retry.WithTryTimeout(time.Second/30),
	)

	var _, err = client.Close(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, 0, counter.Requests())
	require.Equal(t, 6, counter.Closes())
}

// Test_RetryClient_RetryBudgetIsPerCall pins down that one call's failures
// must not poison later calls, and that budget exhaustion no longer cancels
// the process context. Before the fix the budget was a shared struct field:
// 20 failures over the whole process lifetime (across every pod the broker
// serves) permanently poisoned the client and killed the node-wide NSM
// broker via the injected cancel.
func Test_RetryClient_RetryBudgetIsPerCall(t *testing.T) {
	t.Cleanup(func() { goleak.VerifyNone(t) })

	var counter = new(count.Client)
	var cancelCalled int32

	var client = retry.NewClient(
		chain.NewNetworkServiceClient(
			counter,
			&remoteSideClient{
				delay:            time.Millisecond,
				failRequestCount: 1000,
			},
		),
		func() { atomic.AddInt32(&cancelCalled, 1) },
		retry.WithInterval(time.Millisecond),
		retry.WithTryTimeout(time.Second/30),
		retry.WithMaxRetry(4),
	)

	_, err := client.Request(context.Background(), nil)
	require.Error(t, err)
	require.Equal(t, 4, counter.Requests())

	_, err = client.Request(context.Background(), nil)
	require.Error(t, err)
	require.Equal(t, 8, counter.Requests())

	require.Zero(t, atomic.LoadInt32(&cancelCalled))
}

// Test_RetryClient_CloseGivesUpEventually pins down that Close is bounded.
// An endpoint that is already gone never acknowledges the Close; before the
// fix the loop retried forever, and a teardown storm kept hammering the
// whole chain, starving healthy connections' token refreshes.
func Test_RetryClient_CloseGivesUpEventually(t *testing.T) {
	t.Cleanup(func() { goleak.VerifyNone(t) })

	var counter = new(count.Client)

	var client = retry.NewClient(
		chain.NewNetworkServiceClient(
			counter,
			&remoteSideClient{
				delay:          time.Millisecond,
				failCloseCount: 1000,
			},
		),
		nil,
		retry.WithInterval(time.Millisecond),
		retry.WithTryTimeout(time.Second/30),
		retry.WithCloseMaxRetry(3),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.Close(ctx, nil)
	require.Error(t, err)
	require.Equal(t, 3, counter.Closes())
}
