package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/Netcracker/dbaas-opensearch-adapter/client"
	"github.com/Netcracker/dbaas-opensearch-adapter/common"
	"github.com/Netcracker/dbaas-opensearch-adapter/server"
	"log"
	"os"
	"strconv"
	"strings"
)

var (
	tlsEnabled, _   = strconv.ParseBool(common.GetEnv("TLS_ENABLED", "false"))
	adapterPort     = 8080
	adapterProtocol = common.Http
	adapterUsername = common.GetEnv("DBAAS_ADAPTER_USERNAME", "dbaas-aggregator")
	adapterPassword = common.GetEnv("DBAAS_ADAPTER_PASSWORD", "dbaas-aggregator")
	adapterAddress  = common.GetEnv("DBAAS_ADAPTER_ADDRESS", "")

	buildstamp  string
	githash     string
	mode        = flag.String("mode", "", "Specify \"shell\" to run in shell mode, shell mode would also be enabled if first argument is sh")
	interactive = flag.Bool("i", false, "Enables shell mode")
	command     = flag.String("c", "", "Command to run in shell mode")

	logger = common.GetLogger()
)

func main() {
	logger.Info(fmt.Sprintf("Run build %s / %s with %+v ...", buildstamp, githash, os.Args))
	flag.Parse()
	if tlsEnabled {
		adapterPort = 8443
		adapterProtocol = common.Https
	}
	cl := client.NewAdapterClient(adapterProtocol, "", adapterPort, adapterUsername, adapterPassword)
	if *command != "" {
		if cl.Exec(*command) {
			return
		}
	}
	if *interactive || *mode == "shell" || (*mode == "" && (os.Args[0] == "sh" || strings.HasSuffix(os.Args[0], "/sh"))) {
		reader := bufio.NewReader(os.Stdin)
		var enteredCommand string
		for enteredCommand != "exit" {
			terminal(reader, cl)
		}
		return
	}

	log.Fatalln("Fatal error", server.Server(adapterAddress, adapterUsername, adapterPassword))
}

func terminal(reader *bufio.Reader, cl *client.AdapterClient) {
	defer func() { // error handler, when error occurred in command processing
		if err := recover(); err != nil {
			fmt.Printf("Error during command execution: %v\n", err)
		}
	}()
	fmt.Print("dbaas_opensearch> ")
	line, _ := reader.ReadString('\n')
	fmt.Println(line)
	cl.Exec(line)
}
