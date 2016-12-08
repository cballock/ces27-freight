package main

import (
	"flag"
    "github.com/cballock/ita/ce-288/freight/client/infra"
)

var (
    localAddress = flag.String("addr", "localhost", "IP address for incoming client connections")
	localPort    = flag.String("port", "2000", "TCP port for incoming client connections")
)

func main() {
	var (
		hostname string
        server   *infra.Server
		console  *infra.Console
	)

	flag.Parse()

	hostname = *localAddress + ":" + *localPort

    server = infra.NewServer(hostname)
	console = infra.NewConsole(server)
    
    // Spawn goroutines to handle both interfaces
	go server.Run()
	go console.Run()
    
    // Wait fo the server to finish
	<-server.Done()
}