package infra

import (
    "log"
    "net"
    "net/rpc"
    "sync"
    "github.com/cballock/ita/ce-288/freight/api"
)

// Server is the struct that hold all information that is used by this instance of dynamo server.
type Server struct {
    // Network
    listener     net.Listener
    connType     string
    connHostname string

    // Context
    err  error
    done chan struct{}
    
    carriersMutex sync.Mutex
    carriers map[string][]string
    nextOrderNumber int
}

// Create a new server object and return a pointer to it.
func NewServer(hostname string) *Server {
    var server Server
    server.done = make(chan struct{})
    server.connType = "tcp"
    server.connHostname = hostname
    server.carriers = make(map[string][]string)
    server.nextOrderNumber = 1
    return &server
}

// Run a TCP server.
func (server *Server) Run() {
    log.Printf("Running TCP Server.\n")

    // Start RPC server.
    rpcs := NewRPC(server)
    rpc.Register(rpcs)

    // Start the network interface
    go server.Start()
}

// Start will create a new listener on the given interface and handle all the connections afterwards until the server is stopped.
func (server *Server) Start() {
    var (
        err error
    )

    server.listener, err = net.Listen(server.connType, server.connHostname)

    if err != nil {
        server.err = err
        close(server.done)
        return
    }

    log.Printf("Listening on '%v'.\n", server.connHostname)

    for {
        conn, err := server.listener.Accept()

        if err != nil {
            return
        }

        go rpc.ServeConn(conn)
    }
}

// Stop will close the listener create in the Start method, causing it to return.
func (server *Server) Stop() {
    log.Printf("Server stopped.\n")
    server.listener.Close()
}

// Done returns a channel that is closed when server is done
func (server *Server) Done() <-chan struct{} {
    return server.done
}

// Err indicates why this context was canceled
func (server *Server) Err() error {
    return server.err
}

// RegisterCarrier will register a carrier on this server.
func (server *Server) RegisterCarrier(carrierName string, hostname string) {
    
    server.carriersMutex.Lock()
    
    log.Printf("Registering hostname '%v' for carrier '%v'.\n", hostname, carrierName)
    
    if server.carriers[carrierName]==nil {
        server.carriers[carrierName] = make([]string, 0, 10)
    }
    
    registeredHostnames := server.carriers[carrierName]
    
    found := false
    for _, registeredHostname := range registeredHostnames {
        if registeredHostname==hostname {
            found = true
            break
        }
    }
    
    if !found {
        server.carriers[carrierName] = append(server.carriers[carrierName], hostname)
    }
    
    server.logCarriers()
    server.carriersMutex.Unlock()
}

func (server *Server) logCarriers() {
    for carrierName, hostnames := range server.carriers {
        log.Printf("Registered hostnames for carrier '%v': '%v'.\n", carrierName, hostnames)
    }
}

func (server *Server) NewOrderNumber() int {
    orderNumber := server.nextOrderNumber
    server.nextOrderNumber++
    return orderNumber
}

// response is a value object to be passed around in channel inside the NewOrder operation.
type response struct {
    hostname string
    cost     float64
    message  string
}

func (server *Server) NewOrder(destination string, date string, tons float64) []response {
    
    var (
        carriersNumber int
        orderNumber int
        responseChan chan *response
        wg sync.WaitGroup
        responses []response
    )
    
    server.carriersMutex.Lock()
    
    carriersNumber = len(server.carriers)
    responses = make([]response, 0)
    
    if carriersNumber==0 {
        log.Printf("No registered carriers.\n")
        server.carriersMutex.Unlock()
        return responses
    }
    
    responseChan = make(chan *response, carriersNumber)
    
    orderNumber = server.NewOrderNumber()
    
    log.Printf("New order: %v.\n", orderNumber)
    
    for carrierName, _ := range server.carriers {
        wg.Add(1)
        go server.newCarrierOrder(carrierName, orderNumber, destination, date, tons, &wg, responseChan)
    }
    
    wg.Wait()
    close(responseChan)
    server.carriersMutex.Unlock()
    
    log.Printf("All carriers responded.\n")
    for response := range responseChan {
        responses = append(responses, *response)
    }
    
    return responses
}

func (server *Server) newCarrierOrder(carrierName string, orderNumber int, destination string, date string, tons float64, wg *sync.WaitGroup, responseChan chan *response) {
    
    var (
        reply       api.TransportOrderReply
        resp        *response
        responded   bool
        hostname    string
        hostsNumber int
    )
    
    hostnames := server.carriers[carrierName]
    hostsNumber = len(hostnames)
    
    responded = false
    for _, hostname = range hostnames {
        err := server.CallHost(hostname, "NewOrder", &api.TransportOrderArgs{orderNumber, destination, date, tons, (hostsNumber/2+1)}, &reply)
        if err != nil {
            log.Printf("Failed to call server: '%v'.\n", hostname)
        } else {
            responded = true
            break
        }
    }
    
    if !responded {
        resp = &response{carrierName, 0, "Operation failed."}
    } else {
        resp = &response{carrierName, reply.Cost, reply.Message}
    }
    responseChan <- resp
    
    wg.Done()    
}

// ***************
// Communication
// ***************

// CallHost will communicate to another host through it's RPC API.
func (server *Server) CallHost(hostname string, method string, args interface{}, reply interface{}) error {
    var (
        err    error
        client *rpc.Client
    )

    client, err = rpc.Dial(server.connType, hostname)
    if err != nil {
        return err

    }
    defer client.Close()

    err = client.Call("RPC."+method, args, reply)

    if err != nil {
        return err
    }

    return nil
}