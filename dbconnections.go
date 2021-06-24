package main

import (
	"context"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"github.com/jackc/pgx/v4/pgxpool"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	RedisPool *redis.Pool
)

//
// REDIS
//

func grabredisconnection(rl RedisLogin) redis.Conn {
	// to test that we really are pooling:
	// if you uncomment the two Printfs you will see one and only one "called" vs multiple "grabbed"
	if RedisPool == nil {
		poolinit(rl)
		// fmt.Printf("poolinit() called\n")
	}
	connection := RedisPool.Get()
	// fmt.Printf("connection grabbed\n")
	return connection
}

func poolinit(rl RedisLogin) {
	RedisPool = newPool(rl.Addr)
	cleanupHook()
}

func newPool(addr string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		// Dial or DialContext must be set. When both are set, DialContext takes precedence over Dial.
		Dial: func() (redis.Conn, error) { return redis.Dial("tcp", addr) },
	}
}

func cleanupHook() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, syscall.SIGTERM)
	signal.Notify(c, syscall.SIGKILL)
	go func() {
		<-c
		e := RedisPool.Close()
		checkerror(e)
		os.Exit(0)
	}()
}

func rcdel(c redis.Conn, k string) {
	_, err := c.Do("DEL", k)
	checkerror(err)
}

func rcsadd(c redis.Conn, k string, v []byte) {
	_, err := c.Do("SADD", k, v)
	checkerror(err)
}

func rcsetint(c redis.Conn, k string, v int64) {
	_, err := c.Do("SET", k, v)
	checkerror(err)
}

func rcsetstr(c redis.Conn, k string, v string) {
	_, err := c.Do("SET", k, v)
	checkerror(err)
}

func rcpopstr(c redis.Conn, k string) string {
	s, err := redis.String(c.Do("SPOP", k))
	if err != nil {
		s = "SET_IS_EMPTY"
	}
	return s
}

//
// POSTGRESQL
//

func grabpgsqlconnection(pl PostgresLogin, workers int, loglevel int) *pgxpool.Pool {

	url := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", pl.User, pl.Pass, pl.Host, pl.Port, pl.DBName)

	config, oops := pgxpool.ParseConfig(url)
	if oops != nil {
		logiflogging(fmt.Sprintf("Could not execute pgxpool.ParseConfig(url) via %s", url), loglevel, 0)
		panic(oops)
	}

	config.ConnConfig.PreferSimpleProtocol = true
	config.MaxConns = int32((workers + 2) * 2)
	config.MinConns = int32(workers + 2)

	// the boring way if you don't want to go via pgxpool.ParseConfig(url)
	// pooledconnection, err := pgxpool.Connect(context.Background(), url)

	pooledconnection, err := pgxpool.ConnectConfig(context.Background(), config)

	if err != nil {
		logiflogging(fmt.Sprintf("Could not connect to PostgreSQL via %s", url), loglevel, 0)
		panic(err)
	}

	logiflogging(fmt.Sprintf("Connected to %s on PostgreSQL", pl.DBName), loglevel, 2)

	return pooledconnection
}
