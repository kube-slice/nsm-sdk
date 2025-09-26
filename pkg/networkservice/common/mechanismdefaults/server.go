package mechanismdefaults

import (
	"context"
	"fmt"
	"os"
	"syscall"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/networkservicemesh/api/pkg/api/networkservice"
	"github.com/networkservicemesh/sdk/pkg/networkservice/core/next"
)

type mechanismDefaultsServer struct{}

func NewServer() networkservice.NetworkServiceServer {
	return &mechanismDefaultsServer{}
}

func (m *mechanismDefaultsServer) Request(ctx context.Context, connRequest *networkservice.NetworkServiceRequest) (*networkservice.Connection, error) {
	conn := connRequest.GetConnection()
	if conn.Mechanism == nil {
		conn.Mechanism = &networkservice.Mechanism{
			Parameters: make(map[string]string),
		}
	}

	mech := conn.Mechanism
	if mech.Cls == "" {
		mech.Cls = "LOCAL"
	}
	if mech.Type == "" {
		mech.Type = "KERNEL"
	}
	if _, ok := mech.Parameters["inodeURL"]; !ok {
		stat, err := os.Stat("/proc/thread-self/ns/net")
		if err == nil {
			inode := int(stat.Sys().(*syscall.Stat_t).Ino)
			mech.Parameters["inodeURL"] = fmt.Sprintf("inode://4/%d", inode)
		}
	}
	if _, ok := mech.Parameters["name"]; !ok {
		mech.Parameters["name"] = "nsm0"
	}

	return next.Server(ctx).Request(ctx, connRequest)
}

func (m *mechanismDefaultsServer) Close(ctx context.Context, conn *networkservice.Connection) (*empty.Empty, error) {
	return next.Server(ctx).Close(ctx, conn)
}
