package main

import (
	"flag"
	"github.com/cballock/ita/ce-288/freight/carrier/infra"
)

var (
	localAddress  = flag.String("addr", "localhost", "IP address for incoming client connections")
	localPort     = flag.String("port", "3000", "TCP port for incoming client connections")
	ringAddress   = flag.String("ring", "", "Ring coordinator address")
	instanceId    = flag.String("id", "", "Instance id")
    clientAddress = flag.String("client", "localhost:2000", "Client address")
    carrierName   = flag.String("name", "", "Carrier name")
)

// Entry point for our Dynamo server application. This will start a instance of the server, as well as a cache(the in-memory store) and a console that will create a CLI to perform operation on the server.
func main() {
	var (
		hostname string
		id       string
        name     string
        client   string
		cache    *infra.Cache
		server   *infra.Server
		console  *infra.Console
	)

	flag.Parse()

	cache = infra.NewCache()

	hostname = *localAddress + ":" + *localPort
    client = *clientAddress

	// If an id isn't provided, we use the hostname instead
	if *instanceId != "" {
		id = *instanceId
	} else {
		id = hostname
	}
    
    if *carrierName != "" {
        name = *carrierName
    } else if *ringAddress != "" {
        name = *ringAddress
    } else {
        name = hostname
    }
    
    server = infra.NewServer(id, name, hostname, client, cache)
	console = infra.NewConsole(cache, server)

	// Spawn goroutines to handle both interfaces
	go server.Run(*ringAddress)
	go console.Run()

	// Wait fo the server to finish
	<-server.Done()
}
