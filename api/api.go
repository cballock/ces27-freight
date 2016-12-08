package api

type RegisterCarrierArgs struct {
    CarrierName  string
	ConnHostname string
}

type TransportOrderArgs struct {
    OrderNumber  int // Sequential number
	Destination  string // City name
    Date         string // Format: DD/MM/YYYY
    Tons         float64
    Quorum       int
}

type TransportOrderReply struct {
    Cost         float64
    Message      string
}