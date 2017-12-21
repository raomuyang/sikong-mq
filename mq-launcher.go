package main

import (
	"flag"
	"fmt"
	"os"
	"github.com/sikong-mq/skmq"
	"github.com/sikong-mq/skmq/base"
	"encoding/json"
	"io/ioutil"
	"github.com/sikong-mq/skmq/process"
)

func main() {
	confPath := flag.String("conf", base.DefaultConf,
		"The path of queue config.")
	dbConfPath := flag.String("dbconf", base.DefaultDBConf, "The path of redis config file")
	flag.Parse()

	confFile, err := os.Open(*confPath)
	if err != nil {
		fmt.Println("The configuration file was not found")
		os.Exit(1)
	}
	defer confFile.Close()
	loadConf(confFile, process.Configuration)
	res, _ := json.Marshal(process.Configuration)
	fmt.Printf("config   : %s\n", res)

	dbConfFile, err := os.Open(*dbConfPath)
	if err != nil {
		fmt.Println("The redis configuration file was not found, use default config:")
		res, _ := json.Marshal(process.DBConfiguration)
		fmt.Printf("%s\n", res)
	} else {
		defer dbConfFile.Close()
		loadConf(dbConfFile, process.DBConfiguration)
		res, _ := json.Marshal(process.DBConfiguration)
		fmt.Printf("db config: %s\n", res)
	}

	skmq.OpenServer()

}

func loadConf(file *os.File, v interface{})  {
	byteArray, err := ioutil.ReadAll(file)
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(byteArray, v)
	if err != nil {
		panic(err)
	}
}
