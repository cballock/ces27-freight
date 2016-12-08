package infra

import (
	"bufio"
	"log"
	"os"
	"strings"
    "time"
    "strconv"
    "fmt"
    "text/tabwriter"
)

type Console struct {
	server *Server
}

func NewConsole(server *Server) *Console {
	return &Console{server}
}

func (console *Console) Run() {
	var (
		scanner      *bufio.Scanner
		input        string
		tokens       []string
        tokensNumber int
	)

	scanner = bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
        
		input = scanner.Text()
		tokens = strings.Split(input, " ")
        tokensNumber = len(tokens)

		switch tokens[0] {
		case "order":
            if tokensNumber!= 4 {
                log.Println("Usage: order <destination> <date> <tons>")
            } else {
                
                destination := tokens[1]
                date := tokens[2]
                
                if isValidDate(date) {
                    log.Println("Invalid date. Date must be in future and must have format 'DD/MM/YYYY'.")
                } else {
                    tons, tonsErr := strconv.ParseFloat(tokens[3], 64)
                    if tonsErr!=nil || tons<=0 {
                        log.Println("Invalid tons. Must be a positive numeric.")
                    } else {
                        responses := console.server.NewOrder(destination, date, tons)
                        if len(responses)>0 {
                            w := tabwriter.NewWriter(os.Stdout, 5, 0, 1, ' ', tabwriter.Debug)
                            fmt.Fprintf(w, "CARRIER\tCOST\tMESSAGE\t\n")
                            for _, response := range responses {
                                cost := ""
                                if response.cost>0 {
                                    cost = "$" + strconv.FormatFloat(response.cost, 'f', 2, 64)
                                }
                                fmt.Fprintf(w, "%v\t%v\t%v\t\n", response.hostname, cost, response.message)
                            }
                            w.Flush()
                        }
                    }
                }
            }
		}
	}
}	

func isValidDate(dateString string) bool {
    date, err := time.Parse("02/01/2006", dateString)
    if err!=nil {
        return false
    } else if date.After(time.Now()) { // Date must be after today;
        return false
    } else {
        return true
    }
}
