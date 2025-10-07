// Package ensureexpires provides chain elements to update Connection.Path
package ensureexpires

import (
	"context"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/networkservicemesh/api/pkg/api/networkservice"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/networkservicemesh/sdk/pkg/networkservice/core/next"
)

// updateExpiresServer sets Expires on Connection.Path segments
type updateExpiresServer struct {
	ttl time.Duration
}

// NewServer - creates a NetworkServiceServer chain element to update the Connection expiration information
// ttl - time-to-live duration to set Expires; use 0 to skip setting expirations
func NewServer(ttl time.Duration) networkservice.NetworkServiceServer {
	return &updateExpiresServer{
		ttl: ttl,
	}
}

// setExpires sets expiration time on previous and current path segments
func (u *updateExpiresServer) setExpires(conn *networkservice.Connection) {
	if conn == nil || conn.Path == nil || u.ttl == 0 {
		return
	}

	now := time.Now()

	// Previous path segment
	if prev := conn.GetPrevPathSegment(); prev != nil {
		prev.Expires = timestamppb.New(now.Add(u.ttl))
	}

	// Current path segment
	//if len(conn.Path.PathSegments) > 0 && int(conn.Path.GetIndex()) < len(conn.Path.PathSegments) {
	//	curr := conn.Path.PathSegments[conn.Path.GetIndex()]
	//	if curr.Expires == nil {
	//		curr.Expires = timestamppb.New(now.Add(u.ttl))
	//	}
	//}
}

// Request sets Expires and forwards the request
func (u *updateExpiresServer) Request(ctx context.Context, request *networkservice.NetworkServiceRequest) (*networkservice.Connection, error) {
	conn := request.GetConnection()
	if conn == nil {
		conn = &networkservice.Connection{}
		request.Connection = conn
	}
	if conn.Path == nil {
		conn.Path = &networkservice.Path{}
	}

	u.setExpires(conn)
	return next.Server(ctx).Request(ctx, request)
}

// Close sets Expires and forwards the close
func (u *updateExpiresServer) Close(ctx context.Context, conn *networkservice.Connection) (*empty.Empty, error) {
	if conn == nil {
		conn = &networkservice.Connection{}
	}
	if conn.Path == nil {
		conn.Path = &networkservice.Path{}
	}

	u.setExpires(conn)
	return next.Server(ctx).Close(ctx, conn)
}
