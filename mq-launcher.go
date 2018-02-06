package main

import (
	"flag"
	"fmt"
	"os"
	"encoding/json"
	"net/http"
	_ "net/http/pprof"
	"github.com/sikong-mq/skmq"
	"github.com/sikong-mq/skmq/base"
	"github.com/sikong-mq/skmq/process"
)

const DebugPort  = 7185

func main() {
	LoadConf()
	go func() {
		http.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", DebugPort), nil)
	}()

	process.LoggerSetup(nil, skmq.Configuration.LogLevel)
	skmq.OpenServer()
}

func LoadConf() {
	confPath := flag.String("conf", base.DefaultConf,
		"The path of queue config.")
	dbConfPath := flag.String("dbconf", base.DefaultDBConf, "The path of redis config file")
	flag.Parse()

	err := base.UnmarshalJsonFile(skmq.Configuration, *confPath)
	if err != nil {
		fmt.Printf("Error configuration: %s\n", *confPath)
		os.Exit(1)
	}
	res, _ := json.Marshal(skmq.Configuration)
	fmt.Printf("config   : %s\n", res)


	err = base.UnmarshalJsonFile(skmq.DBConfiguration, *dbConfPath)
	if err != nil {
		fmt.Println("The redis configuration was not found, use default config:")
		res, _ := json.Marshal(skmq.DBConfiguration)
		fmt.Printf("%s\n", res)
	} else {
		res, _ := json.Marshal(skmq.DBConfiguration)
		fmt.Printf("db config: %s\n", res)
	}
}
