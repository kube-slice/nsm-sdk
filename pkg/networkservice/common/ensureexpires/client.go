package ensureexpires

import (
	"context"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/networkservicemesh/api/pkg/api/networkservice"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/networkservicemesh/sdk/pkg/networkservice/core/next"
)

// ensureExpiresClient sets Expires for previous and current path segments
type ensureExpiresClient struct {
	ttl time.Duration // 0 = disable
}

// NewClient creates a NetworkServiceClient that sets Expires on previous and current path segment
func NewClient(ttl time.Duration) networkservice.NetworkServiceClient {
	return &ensureExpiresClient{ttl: ttl}
}

// setExpires sets the Expires timestamp for prev and current path segments
func (c *ensureExpiresClient) setExpires(conn *networkservice.Connection) {
	if c.ttl == 0 || conn == nil || conn.Path == nil {
		return
	}
	now := time.Now()
	// Previous path segment
	if prev := conn.GetPrevPathSegment(); prev != nil {
		prev.Expires = timestamppb.New(now.Add(c.ttl))
	}
	// Current path segment (last in PathSegments)
	if len(conn.Path.PathSegments) > 0 {
		curr := conn.Path.PathSegments[conn.Path.GetIndex()]
		curr.Expires = timestamppb.New(now.Add(c.ttl))
	}
}

// Request sets Expires and forwards the request
func (c *ensureExpiresClient) Request(ctx context.Context, request *networkservice.NetworkServiceRequest, opts ...grpc.CallOption) (*networkservice.Connection, error) {
	conn := request.GetConnection()
	if conn == nil {
		conn = &networkservice.Connection{}
		request.Connection = conn
	}
	if conn.Path == nil {
		conn.Path = &networkservice.Path{}
	}

	c.setExpires(conn)

	return next.Client(ctx).Request(ctx, request, opts...)
}

// Close sets Expires and forwards the close call
func (c *ensureExpiresClient) Close(ctx context.Context, conn *networkservice.Connection, opts ...grpc.CallOption) (*empty.Empty, error) {
	if conn == nil {
		conn = &networkservice.Connection{}
	}
	if conn.Path == nil {
		conn.Path = &networkservice.Path{}
	}

	c.setExpires(conn)

	return next.Client(ctx).Close(ctx, conn, opts...)
}
