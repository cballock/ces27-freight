package infra

import (
    "errors"
    "log"
    "time"
    "net"
    "net/rpc"
    "strconv"
    "strings"
    "math"
    "github.com/cballock/ita/ce-288/freight/api"
)

// Server is the struct that hold all information that is used by this instance of dynamo server.
type Server struct {
    // Network
    listener     net.Listener
    connType     string
    connHostname string

    // Partitioning
    id       string
    ring     *Ring
    replicas int

    // Context
    err  error
    done chan struct{}

    // Storage
    cache *Cache
    
    client string
    carrierName string
}

// Create a new server object and return a pointer to it.
func NewServer(id string, carrierName string, hostname string, client string, cache *Cache) *Server {
    var server Server
    server.done = make(chan struct{})
    server.cache = cache
    server.replicas = 3
    server.connType = "tcp"
    server.connHostname = hostname
    server.id = id
    server.ring = NewRing(&server)
    server.carrierName = carrierName
    server.client = client
    return &server
}

// Run a dynamo server. The parameter joinAddress is the address to the server which this instance should use get the current status of the consistenthash ring.
func (server *Server) Run(joinAddress string) {
    log.Printf("Running Dynamo Server with id '%v'.\n", server.id)

    if joinAddress != "" {
        server.ring.Sync(joinAddress)
    } else {
        server.ring.AddNode(server.id, server.connHostname)
    }

    // Start RPC servers.
    // RPC is the public API and InternalRPC is the internal server-to-server
    // API.
    rpcs := NewRPC(server)
    internalrpcs := NewInternalRPC(server)
    rpc.Register(rpcs)
    rpc.Register(internalrpcs)

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
    
    log.Printf("Registering server '%v'.\n", server.connHostname)
    server.Register()

    log.Printf("Listening on '%v'.\n", server.connHostname)

    for {
        conn, err := server.listener.Accept()

        if err != nil {
            return
        }

        go rpc.ServeConn(conn)
    }
}

// Register carrier.
func (server *Server) Register() {
    err := server.CallHost(server.client, "RegisterCarrier", &api.RegisterCarrierArgs{server.carrierName, server.connHostname}, nil)
    
    if err != nil {
        log.Printf("Failed to register server.\n", )
    }
}

// Stop will close the listener create in the Start method, causing it to return.
func (server *Server) Stop() {
    log.Printf("Server Stopped.\n")
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

// ********
// Router
// ********

// RoutePut will handle the search for a coordinator of a Put operation. It'll start a replication if it finds itself as coordinator or delegate it to the remote coordinator and wait for it.
func (server *Server) RoutePut(key string, value string, quorum int) error {
	var (
		err                 error
		coordinatorId       string
		coordinatorHostname string
	)

	log.Printf("Routing Put of KV('%v', '%v') with quorum Q('%v').\n", key, value, quorum)

	coordinatorId, coordinatorHostname = server.ring.GetCoordinator(key)

	for server.id != coordinatorId {
		log.Printf("Trying '%v' as coordinator.\n", coordinatorId)
		err = server.CallInternalHost(coordinatorHostname, "CoordinatePut", &CoordinatePutArgs{key, value, quorum}, nil)

		if err == nil {
			log.Printf("Coordinate succeded.\n")
			break
		}

		if err == notEnoughQuorumErr {
			log.Printf("Not enough quorum error.\n")
			return err
		}

		log.Printf("Coordinator tryout failed. Error: '%v'.\n", err)

		coordinatorId, coordinatorHostname, err = server.ring.GetNextCoordinator(coordinatorId)

		if err != nil {
			log.Printf("Failed to find next coordinator to '%v'.\n", coordinatorId)
			return err
		}
	}

	if server.id == coordinatorId {
		err = server.Replicate(key, value, quorum)
	}

	return err
}

// RouteGet will handle the search for a coordinator of a Get operation. It'll start a votation if it finds itself as coordinator or delegate the it to the remote coordinator and wait for it.
func (server *Server) RouteGet(key string, quorum int) (value string, err error) {
	var (
		coordinatorId       string
		coordinatorHostname string
		reply               CoordinateGetReply
	)

	log.Printf("Routing Get of K('%v') with quorum Q('%v').\n", key, quorum)

	coordinatorId, coordinatorHostname = server.ring.GetCoordinator(key)

	for server.id != coordinatorId {
		log.Printf("Trying '%v' as coordinator.\n", coordinatorId)
		err = server.CallInternalHost(coordinatorHostname, "CoordinateGet", &CoordinateGetArgs{key, quorum}, &reply)

		if err == nil {
			log.Printf("Coordinate succeded.\n")
			value = reply.Value
			break
		}

		if err == notEnoughQuorumErr {
			log.Printf("Not enough quorum error.\n")
			return "", err
		}

		log.Printf("Coordinator tryout failed. Error: '%v'.\n", err)

		coordinatorId, coordinatorHostname, err = server.ring.GetNextCoordinator(coordinatorId)

		if err != nil {
			log.Printf("Failed to find next coordinator to '%v'.\n", coordinatorId)
			return "", err
		}
	}

	if server.id == coordinatorId {
		value, err = server.Voting(key, quorum)

		if err != nil {
			return "", err
		}
	}

	return value, nil
}

// NewOrder will calculate the transport order cost.
func (server *Server) NewOrder(orderNumber int, destination string, date string, tons float64, quorum int) (cost float64, message string, err error) {
    
    var (
        rule            string
        rates           []string
        perTonRate      float64
        perTripRate     float64
        trucks          string
        availableTrucks int64
        requestedTrucks int64
    )
    
    rule, err = server.RouteGet("city:" + destination, quorum)
    
    if err!=nil {
        cost = 0
        message = "Error retrieving city."
        server.logOrder(orderNumber, destination, date, tons, cost, message, quorum)
        return cost, message, err
    }
    
    if rule=="" {
        cost = 0
        message = "Destination unattended."
        server.logOrder(orderNumber, destination, date, tons, cost, message, quorum)
        return cost, message, nil
    }
    
    rates = strings.Split(rule, ";")
    perTonRate, err = strconv.ParseFloat(rates[0], 64)
    
    if err!=nil {
        cost = 0
        message = "Invalid value for per ton rate."
        server.logOrder(orderNumber, destination, date, tons, cost, message, quorum)
        return cost, message, err
    }
    
    perTripRate, err = strconv.ParseFloat(rates[1], 64)
    
    if err!=nil {
        cost = 0
        message = "Invalid value for per trip rate."
        server.logOrder(orderNumber, destination, date, tons, cost, message, quorum)
        return cost, message, err
    }
    
    trucks, err = server.RouteGet("trucks:" + date, quorum)
    
    if err!=nil {
        cost = 0
        message = "Error retrieving available trucks."
        server.logOrder(orderNumber, destination, date, tons, cost, message, quorum)
        return cost, message, err
    }
    
    if trucks=="" {
        cost = 0
        message = "Unavailable trucks for requested date."
        server.logOrder(orderNumber, destination, date, tons, cost, message, quorum)
        return cost, message, nil
    }
    
    availableTrucks, err = strconv.ParseInt(trucks, 10, 0)
    
    if err!=nil {
        cost = 0
        message = "Invalid value for available trucks."
        server.logOrder(orderNumber, destination, date, tons, cost, message, quorum)
        return cost, message, err
    }
    
    requestedTrucks = int64(math.Ceil(tons/45))
    if availableTrucks<requestedTrucks {
        cost = 0
        message = "Unavailable trucks for requested date."
        server.logOrder(orderNumber, destination, date, tons, cost, message, quorum)
        return cost, message, nil
    }
    
    cost = tons*perTonRate + float64(requestedTrucks)*perTripRate
    err = server.logOrder(orderNumber, destination, date, tons, cost, message, quorum)
    
    if err!=nil {
        message = "Error saving order."
        server.logOrder(orderNumber, destination, date, tons, cost, message, quorum)
        return cost, message, err
    }
    
    err = server.RoutePut("trucks:" + date, strconv.FormatInt(availableTrucks-requestedTrucks, 10), quorum)
    
    if err!=nil {
        message = "Error updating available trucks."
        server.logOrder(orderNumber, destination, date, tons, cost, message, quorum)
        return cost, message, err
    }
    
    return cost, "", nil
}

func (server *Server) logOrder(orderNumber int, destination string, date string, tons float64, cost float64, message string, quorum int) error {
    key := "order:" + strconv.Itoa(orderNumber)
    value := "dest=" + destination + "; date=" + date + "; tons=" + strconv.FormatFloat(tons, 'f', 2, 64) + "; cost=" + strconv.FormatFloat(cost, 'f', 2, 64) + "; message='" + message + "'."
    return server.RoutePut(key, value, quorum)
}

// *************
// Coordinator
// *************

var notEnoughQuorumErr = errors.New("Not enough quorum")

// Replicate will coordinate the replication("sharding") of the data into the preferred nodes.
func (server *Server) Replicate(key string, value string, quorum int) error {
	var (
		err        error
		nodes      []string
		reportChan chan error
		successes  int
		failures   int
		baseNode   string
		timestamp  int64
	)

	log.Printf("Coordinating replication of KV('%v', '%v') with quorum '%v'.\n", key, value, quorum)

	timestamp = time.Now().Unix()
	log.Printf("Operation timestamp: '%v'.\n", timestamp)

	baseNode, _ = server.ring.GetNode(key)

	nodes, err = server.ring.GetNodes(baseNode, server.replicas)

	if err != nil {
		log.Printf("Failed to replicate nodes. Error: '%v'.\n", err)
		return err
	}

	reportChan = make(chan error, server.replicas)
	for _, node := range nodes {
		if server.connHostname == node {
			go func() {
				server.cache.Put(key, value, timestamp)

				reportChan <- nil
			}()
		} else {
			go func(hostname string) {
				var err error

				err = server.CallInternalHost(hostname, "Replicate", &ReplicateArgs{key, value, timestamp}, nil)

				reportChan <- err
			}(node)
		}
	}

	successes = 0
	failures = 0
	for report := range reportChan {
		if report != nil {
			log.Printf("Error on replication: '%v'.\n", report)
			failures++
		} else {
			successes++
		}

		if successes == quorum {
			log.Printf("Replication with quorum '%v' succeded.\n", quorum)
			break
		}

		if failures+successes == len(nodes) {
			log.Printf("Replication failed. Not enough quorum.\n")
			return notEnoughQuorumErr
		}
	}

	go func() {
		for report := range reportChan {
			if report != nil {
				log.Printf("Error on replication: '%v'.\n", report)
				continue
			}
		}
	}()

	return nil
}

// vote is a value object to be passed around in channel inside the Voting operation.
type vote struct {
	value     string
	timestamp int64
	err       error
}

// Replicate will coordinate the voting(to decide on a value to be returned) of the data replicated in the preferred nodes.
func (server *Server) Voting(key string, quorum int) (string, error) {
	var (
		err        error
		nodes      []string
		reportChan chan *vote
		successes  int
		failures   int
		votes      []*vote
		baseNode   string
	)

	log.Printf("Coordinating voting of K('%v') with quorum '%v'.\n", key, quorum)

	baseNode, _ = server.ring.GetNode(key)

	nodes, err = server.ring.GetNodes(baseNode, server.replicas)

	if err != nil {
		log.Printf("Failed to gather votes from nodes.\n")
		return "", err
	}

	reportChan = make(chan *vote, server.replicas)

	for _, node := range nodes {
		if server.connHostname == node {
			go func() {
				var (
					v         *vote
					value     string
					timestamp int64
				)
				value, timestamp = server.cache.Get(key)
				v = &vote{value, timestamp, nil}
				reportChan <- v
			}()
		} else {
			go func(hostname string) {
				var (
					err   error
					reply VoteReply
					v     *vote
				)

				err = server.CallInternalHost(hostname, "Vote", &VoteArgs{key}, &reply)

				v = &vote{reply.Value, reply.Timestamp, err}
				reportChan <- v
			}(node)
		}
	}

	successes = 0
	failures = 0
	votes = make([]*vote, 0)
	for report := range reportChan {
		if report.err != nil {
			log.Printf("Error on vote: '%v'.\n", report.err)
			failures++
		} else {
			successes++
			votes = append(votes, report)
		}

		if successes == quorum {
			log.Printf("Voting with quorum '%v' succeded.\n", quorum)
			break
		}

		if failures+successes == len(nodes) {
			log.Printf("Voting failed. Not enough quorum.\n")
			return "", notEnoughQuorumErr
		}
	}

	go func() {
		for report := range reportChan {
			if report.err != nil {
				log.Printf("Error on vote: '%v'.\n", report.err)
				continue
			}
		}
	}()

	return aggregateVotes(votes), nil
}

// aggregateVotes will select the right value from the votes received.
func aggregateVotes(votes []*vote) (result string) {
    selectedVote := votes[0]
	for _, vote := range votes {
		log.Printf("Vote: '%v'.\n", vote.value)
        if vote.timestamp > selectedVote.timestamp {
            selectedVote = vote
        }
	}
	return selectedVote.value
}

// ***************
// Communication
// ***************

// CallInternalHost will communicate to another host through it's InternalRPC API.
func (server *Server) CallInternalHost(hostname string, method string, args interface{}, reply interface{}) error {
    var (
        err    error
        client *rpc.Client
    )

    client, err = rpc.Dial(server.connType, hostname)
    if err != nil {
        return err

    }
    defer client.Close()

    err = client.Call("InternalRPC."+method, args, reply)

    if err != nil {
        return err
    }

    return nil
}

// CallHost will communicate to another host through it's RPC public API.
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