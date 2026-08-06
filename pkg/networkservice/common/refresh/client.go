// Copyright (c) 2020 Cisco Systems, Inc.
//
// Copyright (c) 2020-2022 Doc.ai and/or its affiliates.
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

// Package refresh periodically resends NetworkServiceMesh.Request for an
// existing connection so that the Endpoint doesn't 'expire' the networkservice.
package refresh

import (
	"context"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"

	"github.com/networkservicemesh/api/pkg/api/networkservice"

	"github.com/networkservicemesh/sdk/pkg/networkservice/common/begin"
	"github.com/networkservicemesh/sdk/pkg/networkservice/core/next"
	"github.com/networkservicemesh/sdk/pkg/networkservice/utils/metadata"
	"github.com/networkservicemesh/sdk/pkg/tools/clock"
	"github.com/networkservicemesh/sdk/pkg/tools/log"
)

const (
	// retryBackoffStart..retryBackoffCap bound the urgent retry cadence used
	// when a refresh attempt fails while the token is still alive. A failed
	// refresh must not wait a full ticker interval: the ticker fires once per
	// refresh window, so waiting means at most two attempts per token
	// lifetime, and two consecutive failures kill a healthy connection.
	retryBackoffStart = time.Second
	retryBackoffCap   = 10 * time.Second
	// attemptTimeoutCap bounds a single refresh attempt: a wedged downstream
	// (e.g. a Close storm on the nsmgr) must not consume the whole remaining
	// token lifetime on one hanging call.
	attemptTimeoutCap = 10 * time.Second
)

type refreshClient struct {
	chainCtx context.Context
}

// NewClient - creates new NetworkServiceClient chain element for refreshing
// connections before they timeout at the endpoint.
func NewClient(ctx context.Context) networkservice.NetworkServiceClient {
	return &refreshClient{
		chainCtx: ctx,
	}
}

func (t *refreshClient) Request(ctx context.Context, request *networkservice.NetworkServiceRequest, opts ...grpc.CallOption) (*networkservice.Connection, error) {
	logger := log.FromContext(ctx).WithField("refreshClient", "Request")

	conn, err := next.Client(ctx).Request(ctx, request, opts...)
	if err != nil {
		return nil, err
	}

	// Compute refreshAfter and the hard expiry of the shortest-lived path
	// segment. The latter is the deadline the urgent retry loop races.
	refreshAfter, expireTime := after(ctx, conn)

	// Create a cancel context.
	cancelCtx, cancel := context.WithCancel(t.chainCtx)

	if oldCancel, loaded := loadAndDelete(ctx, metadata.IsClient(t)); loaded {
		oldCancel()
	}
	store(ctx, metadata.IsClient(t), cancel)

	eventFactory := begin.FromContext(ctx)
	clockTime := clock.FromContext(ctx)
	// Create the afterCh *outside* the go routine.  This must be done to avoid picking up a later 'now'
	// from mockClock in testing
	afterTicker := clockTime.Ticker(refreshAfter)
	go func() {
		defer afterTicker.Stop()
		for {
			select {
			case <-cancelCtx.Done():
				return
			case <-afterTicker.C():
				if refreshUrgently(cancelCtx, eventFactory, clockTime, logger, expireTime) {
					// The successful Request re-entered this element and
					// armed a fresh goroutine with the new expiry.
					return
				}
				// Every attempt failed and the token expired. Keep trying at
				// the ticker cadence: the path may still recover, and a
				// successful Request re-arms this element with fresh state.
			}
		}
	}()

	return conn, nil
}

// refreshUrgently re-requests the connection until a refresh succeeds or the
// token expires. A refresh failure is not a connection failure: the datapath
// is still up and only the token is at risk, so retry quickly (bounded
// backoff), bound each attempt (a hanging downstream must not eat the whole
// window), and never wait a full ticker interval while the token burns down.
// Returns true once a refresh succeeded.
func refreshUrgently(cancelCtx context.Context, eventFactory begin.EventFactory, clockTime clock.Clock, logger log.Logger, expireTime time.Time) bool {
	backoff := retryBackoffStart
	for first := true; ; first = false {
		// The first attempt of a refresh window keeps the historical
		// semantics: no extra deadline beyond the chain's own timeouts.
		// Only the urgent retries are individually bounded, so a wedged
		// downstream cannot consume the whole remaining token lifetime.
		// Check for cancellation *before* attempting, never after: a
		// successful refresh re-enters this element, which cancels this
		// goroutine's context by design, so a post-attempt check would
		// misreport every success as "refresh failed: context canceled".
		if cancelCtx.Err() != nil {
			return false
		}

		attemptCtx, attemptCancel := context.Context(cancelCtx), context.CancelFunc(func() {})
		if !first {
			attemptCtx, attemptCancel = attemptContext(cancelCtx, clockTime, expireTime)
		}
		err := <-eventFactory.Request(begin.CancelContext(attemptCtx))
		attemptCancel()
		if err == nil {
			return true
		}
		logger.Warnf("refresh failed: %s", err.Error())

		if !expireTime.IsZero() && clockTime.Until(expireTime) <= 0 {
			return false
		}

		select {
		case <-cancelCtx.Done():
			return false
		case <-clockTime.After(backoff):
		}

		backoff *= 2
		if backoff > retryBackoffCap {
			backoff = retryBackoffCap
		}
	}
}

// attemptContext bounds one refresh attempt to the lesser of attemptTimeoutCap
// and half the remaining token lifetime, so at least two attempts always fit
// into whatever lifetime is left.
func attemptContext(parent context.Context, clockTime clock.Clock, expireTime time.Time) (context.Context, context.CancelFunc) {
	timeout := attemptTimeoutCap
	if !expireTime.IsZero() {
		if remaining := clockTime.Until(expireTime); remaining > 0 && remaining/2 < timeout {
			timeout = remaining / 2
		}
	}
	return clockTime.WithTimeout(parent, timeout)
}

func (t *refreshClient) Close(ctx context.Context, conn *networkservice.Connection, opts ...grpc.CallOption) (e *empty.Empty, err error) {
	if oldCancel, loaded := loadAndDelete(ctx, metadata.IsClient(t)); loaded {
		oldCancel()
	}
	return next.Client(ctx).Close(ctx, conn, opts...)
}

func after(ctx context.Context, conn *networkservice.Connection) (time.Duration, time.Time) {
	clockTime := clock.FromContext(ctx)

	var minTimeout *time.Duration
	var expireTime time.Time
	for _, segment := range conn.GetPath().GetPathSegments() {
		expTime := segment.GetExpires().AsTime()

		timeout := clockTime.Until(expTime)

		if minTimeout == nil || timeout < *minTimeout {
			if minTimeout == nil {
				minTimeout = new(time.Duration)
			}

			*minTimeout = timeout
			expireTime = expTime
		}
	}

	if minTimeout != nil {
		log.FromContext(ctx).Infof("expiration after %s at %s", minTimeout.String(), expireTime.UTC())
	}

	if minTimeout == nil || *minTimeout <= 0 {
		return 1, expireTime
	}

	// A heuristic to reduce the number of redundant requests in a chain
	// made of refreshing clients with the same expiration time: let outer
	// chain elements refresh slightly faster than inner ones.
	// Update interval is within 0.2*expirationTime .. 0.4*expirationTime
	scale := 1. / 3.
	path := conn.GetPath()
	if len(path.PathSegments) > 1 {
		scale = 0.4 + 0.4*float64(path.Index)/float64(len(path.PathSegments))
	}
	duration := time.Duration(float64(*minTimeout) * scale)

	return duration, expireTime
}
