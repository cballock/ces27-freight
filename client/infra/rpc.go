package infra

import (
    "github.com/cballock/ita/ce-288/freight/api"
)

// RPC is the struct on which the public API operations will be called.
type RPC struct {
	server *Server
}

// NewRPC returns a new RPC struct instance that contains the procedures specified in the public API.
func NewRPC(server *Server) *RPC {
	return &RPC{server}
}

// Connects a Carrier.
func (rpc *RPC) RegisterCarrier(args *api.RegisterCarrierArgs, _ *struct{}) error {
	rpc.server.RegisterCarrier(args.CarrierName, args.ConnHostname)
	return nil
}