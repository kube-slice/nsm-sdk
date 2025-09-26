package mechanismdefaults

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
	"os"
	"syscall"

	"github.com/networkservicemesh/api/pkg/api/networkservice"
	"github.com/networkservicemesh/sdk/pkg/networkservice/core/next"
)

// NewClient returns a NetworkServiceClient that fills in default mechanism values.
func NewClient() networkservice.NetworkServiceClient {
	return &mechanismDefaultsClient{}
}

type mechanismDefaultsClient struct{}

// Request ensures that the Mechanism fields are populated with defaults.
func (m *mechanismDefaultsClient) Request(ctx context.Context, request *networkservice.NetworkServiceRequest, opts ...grpc.CallOption) (*networkservice.Connection, error) {
	mech := request.GetConnection().GetMechanism()
	if mech != nil {
		// Set default class and type if empty
		if mech.Cls == "" {
			mech.Cls = "LOCAL"
		}
		if mech.Type == "" {
			mech.Type = "KERNEL"
		}

		// Ensure inodeURL is set
		if _, ok := mech.Parameters["inodeURL"]; !ok || mech.Parameters["inodeURL"] == "" {
			stat, err := os.Stat("/proc/thread-self/ns/net")
			if err == nil {
				inode := int(stat.Sys().(*syscall.Stat_t).Ino)
				mech.Parameters["inodeURL"] = fmt.Sprintf("inode://4/%d", inode)
			}
		}

		// Ensure interface name is set
		if _, ok := mech.Parameters["name"]; !ok || mech.Parameters["name"] == "" {
			mech.Parameters["name"] = "nsm0"
		}
	}

	return next.Client(ctx).Request(ctx, request, opts...)
}

func (m *mechanismDefaultsClient) Close(ctx context.Context, conn *networkservice.Connection, opts ...grpc.CallOption) (*empty.Empty, error) {
	return next.Client(ctx).Close(ctx, conn, opts...)
}
