package main

import (
	"fmt"
	"github.com/raomuyang/sikong-mq/skmq/base"
	"github.com/raomuyang/sikong-mq/skmq/lock"
	"github.com/raomuyang/sikong-mq/skmq/process"
	"github.com/garyburd/redigo/redis"
)

func DistributedLockDemo() {
	// load conf
	conf := &base.DBConfig{}
	LoadConf(conf, "dbconf.json")

	// init client
	db := process.InitDBConfig(*conf, 1)

	// lock
	res, err := db.Locker().TryLock("test-lock")
	fmt.Println("lock:", res, err)

	err = db.Locker().Unlock("test-lock")
	fmt.Println("unlock: errr:", err)
	// Idempotent
	fmt.Println("unlock again: errr:", db.Locker().Unlock("test-lock"))

	pool := db.Locker().Pool

	// create locker
	locker, _ := CreateLockByPool(pool)
	fmt.Println(locker.TryLock())
	fmt.Println(locker.TryLock())
	fmt.Println(locker.Unlock())
	fmt.Println(locker.Unlock())

}

func CreateLockByPool(pool *redis.Pool) (*lock.RedisLock, error) {
	return lock.NewLock("test", pool, 1000)
}

func LoadConf(conf interface{}, path string) {
	base.UnmarshalJsonFile(conf, path)
}
