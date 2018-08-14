package lock

import (
	"crypto/md5"
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/satori/go.uuid"
)

const (
	PREFIX = "lock->"
	script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end"
)

type RedisLock struct {
	pool       *redis.Pool
	expireTime int
	key        string
	value      string
}

func NewLock(name string, pool *redis.Pool, expireSeconds int) (*RedisLock, error) {
	if name == "" {
		return nil, errors.New("illegal argument: name")
	}
	key := PREFIX + name
	valueContent := uuid.NewV1().String() + key
	md5sum := md5.New()
	md5sum.Write([]byte(valueContent))

	lock := RedisLock{
		key:        key,
		value:      fmt.Sprintf("%x", md5sum.Sum(nil)),
		pool:       pool,
		expireTime: expireSeconds}
	return &lock, nil
}

func (redisLock *RedisLock) TryLock() (bool, error) {
	dbConn := redisLock.pool.Get()
	rep, err := dbConn.Do("SET",
		redisLock.key, redisLock.value, "NX", "EX", redisLock.ExpireTime())
	if err != nil {
		return false, err
	}

	if rep == nil {
		return false, nil
	}
	result, err := redis.String(rep, err)
	if result == "OK" {
		return true, nil
	}
	return false, err
}

func (redisLock *RedisLock) Unlock() error {
	dbConn := redisLock.pool.Get()
	_, err := dbConn.Do("EVAL", script, 1, redisLock.key, redisLock.value)
	return err
}

func (redisLock *RedisLock) ExpireTime() int {
	if redisLock.expireTime == 0 {
		// default one day
		return 60 * 60 * 24
	}
	return redisLock.expireTime
}

func (redisLock *RedisLock) SetExpireTime(expireTime int) {
	redisLock.expireTime = expireTime
}
