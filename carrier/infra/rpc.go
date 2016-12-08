package infra

import (
    "github.com/cballock/ita/ce-288/freight/api"
)

// RPC is the struct on which the public API operations will be called.
type RPC struct {
	server *Server
}

type GetArgs struct {
	Key    string
	Quorum int
}

type GetReply struct {
	Value string
}

type PutArgs struct {
	Key    string
	Value  string
	Quorum int
}

// NewRPC returns a new RPC struct instance that contains the procedures specified in the public API.
func NewRPC(server *Server) *RPC {
	return &RPC{server}
}

// Get handles the read operations on the database. It'll redirect all operations to the router that will find a suitable coordinator for the operation and wait for it to coordinate the operation and return a reply.
func (rpc *RPC) Get(args *GetArgs, reply *GetReply) error {
	var (
		err   error
		value string
	)

	value, err = rpc.server.RouteGet(args.Key, args.Quorum)

	if err != nil {
		return err
	}

	*reply = GetReply{value}
	return nil
}

// Put handles the write operations on the database. It'll redirect all operations to the router that will find a suitable coordinator for the operation and wait for it to coordinate the operation and return.
func (rpc *RPC) Put(args *PutArgs, _ *struct{}) error {
	rpc.server.RoutePut(args.Key, args.Value, args.Quorum)
	return nil
}

// NewOrder returns the cost for a new transport order.
func (rpc *RPC) NewOrder(args *api.TransportOrderArgs, reply *api.TransportOrderReply) error {
    var (
		err   error
		cost  float64
        msg   string
	)
    
	cost, msg, err = rpc.server.NewOrder(args.OrderNumber, args.Destination, args.Date, args.Tons, args.Quorum)

	if err != nil {
		return err
	}

	*reply = api.TransportOrderReply{cost, msg}
	return nil
}
