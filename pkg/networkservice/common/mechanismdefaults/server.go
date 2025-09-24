package mechanismdefaults

import (
	"context"
	"strconv"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/networkservicemesh/api/pkg/api/networkservice"
	"github.com/networkservicemesh/sdk/pkg/networkservice/core/next"
)

// NewServer returns a NetworkServiceServer chain element
// that populates missing integer mechanism parameters for KERNEL mechanisms.
func NewServer() networkservice.NetworkServiceServer {
	return &kernelDefaultServer{}
}

type kernelDefaultServer struct{}

// Request populates integer parameters before forwarding
func (k *kernelDefaultServer) Request(ctx context.Context, request *networkservice.NetworkServiceRequest) (*networkservice.Connection, error) {
	for _, mech := range request.MechanismPreferences {
		populateKernelDefaults(mech)
	}
	if request.GetConnection() != nil {
		populateKernelDefaults(request.GetConnection().GetMechanism())
	}
	return next.Server(ctx).Request(ctx, request)
}

// Close ensures integers exist before closing
func (k *kernelDefaultServer) Close(ctx context.Context, conn *networkservice.Connection) (*empty.Empty, error) {
	populateKernelDefaults(conn.GetMechanism())
	return next.Server(ctx).Close(ctx, conn)
}

// populateKernelDefaults sets default integer parameters for KERNEL mechanisms
func populateKernelDefaults(mech *networkservice.Mechanism) {
	if mech == nil || mech.GetCls() != "LOCAL" || mech.GetType() != "KERNEL" {
		return
	}

	if mech.Parameters == nil {
		mech.Parameters = make(map[string]string)
	}

	// ifindex required by kernel forwarder
	if _, ok := mech.Parameters["ifindex"]; !ok {
		mech.Parameters["ifindex"] = strconv.Itoa(1)
	}

	// inode optional, set default if missing
	if _, ok := mech.Parameters["inode"]; !ok {
		mech.Parameters["inode"] = strconv.Itoa(1)
	}
}
