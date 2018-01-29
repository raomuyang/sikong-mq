package main

import (
	"flag"
	"fmt"
	"os"
	"github.com/sikong-mq/skmq"
	"github.com/sikong-mq/skmq/base"
	"encoding/json"
	"github.com/sikong-mq/skmq/process"
)

func main() {
	LoadConf()
	skmq.OpenServer()
}

func LoadConf() {
	confPath := flag.String("conf", base.DefaultConf,
		"The path of queue config.")
	dbConfPath := flag.String("dbconf", base.DefaultDBConf, "The path of redis config file")
	flag.Parse()

	err := base.UnmarshalJsonFile(process.Configuration, *confPath)
	if err != nil {
		fmt.Printf("Error configuration: %s\n", *confPath)
		os.Exit(1)
	}
	res, _ := json.Marshal(process.Configuration)
	fmt.Printf("config   : %s\n", res)


	err = base.UnmarshalJsonFile(process.DBConfiguration, *dbConfPath)
	if err != nil {
		fmt.Println("The redis configuration was not found, use default config:")
		res, _ := json.Marshal(process.DBConfiguration)
		fmt.Printf("%s\n", res)
	} else {
		res, _ := json.Marshal(process.DBConfiguration)
		fmt.Printf("db config: %s\n", res)
	}
}
