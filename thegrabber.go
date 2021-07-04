//    HipparchiaGoDBHelper: search and vector helper app and functions for HipparchiaServer
//    Copyright: E Gunderson 2021
//    License: GNU GENERAL PUBLIC LICENSE 3
//        (see LICENSE in the top level directory of the distribution)

//
// the GRABBER is supposed to be pointedly basic
// [a] it looks to redis for a pile of SQL queries that were pre-rolled
// [b] it asks postgres to execute these queries
// [c] it stores the results on redis
// [d] it also updates the redis progress poll data relative to this search
//

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"runtime"
	"sync"
)

// HipparchiaGolangSearcher : Execute a series of SQL queries stored in redis by dispatching a collection of goroutines
// the python module calls this; the module needs to be able to set the logging level, etc.
// and so you set up 'cfg' and then you can call HipparchiaSearcher()
func HipparchiaGolangSearcher(thekey string, hitcap int64, workercount int, ll int, rl RedisLogin, pl PostgresLogin) string {
	cfg.RedisKey = thekey
	cfg.MaxHits = hitcap
	cfg.WorkerCount = workercount
	cfg.LogLevel = ll
	cfg.RLogin = rl
	cfg.PGLogin = pl

	resultkey := HipparchiaSearcher()
	return resultkey
}

func HipparchiaSearcher() string {
	msg(fmt.Sprintf("Searcher Launched"), 1)

	runtime.GOMAXPROCS(cfg.WorkerCount + 1)

	recordinitialsizeofworkpile(cfg.RedisKey)

	var awaiting sync.WaitGroup

	for i := 0; i < cfg.WorkerCount; i++ {
		awaiting.Add(1)
		go grabber(i, cfg.RedisKey, &awaiting)
	}

	awaiting.Wait()

	resultkey := cfg.RedisKey + "_results"
	return resultkey
}

func grabber(clientnumber int, searchkey string, awaiting *sync.WaitGroup) {
	// this is where all of the work happens
	defer awaiting.Done()
	msg(fmt.Sprintf("Hello from grabber %d", clientnumber), 3)

	rc := grabredisconnection()
	defer func(rc redis.Conn) {
		err := rc.Close()
		checkerror(err)
	}(rc)

	dbpool := grabpgsqlconnection()
	defer dbpool.Close()

	resultkey := searchkey + "_results"

	for {
		// [i] get a pre-rolled query or break the loop
		thequery := rcpopstr(rc, searchkey)
		if thequery == "SET_IS_EMPTY" {
			break
		}

		// [ii] - [v] inside findtherows() because its code is common with HipparchiaBagger's needs
		foundrows := findtherows(thequery, "grabber", searchkey, clientnumber, rc, dbpool)

		// [vi] iterate through the finds
		// don't check-and-load find-by-find because some searches are effectively uncapped
		// faster to test only after you finish each query
		// can over-stuff redis because HipparchaServer should only display hitcap results no matter how many you push

		var thesefinds []DbWorkline

		defer foundrows.Close()
		for foundrows.Next() {
			// [vi.1] convert the finds into DbWorklines
			var thehit DbWorkline
			err := foundrows.Scan(&thehit.WkUID, &thehit.TbIndex, &thehit.Lvl5Value, &thehit.Lvl4Value, &thehit.Lvl3Value,
				&thehit.Lvl2Value, &thehit.Lvl1Value, &thehit.Lvl0Value, &thehit.MarkedUp, &thehit.Accented,
				&thehit.Stripped, &thehit.Hypenated, &thehit.Annotations)
			checkerror(err)
			thesefinds = append(thesefinds, thehit)
		}

		// [vi.2] load via pipeline
		err := rc.Send("MULTI")
		checkerror(err)

		for i := 0; i < len(thesefinds); i++ {
			jsonhit, ee := json.Marshal(thesefinds[i])
			checkerror(ee)

			e := rc.Send("SADD", resultkey, jsonhit)
			checkerror(e)
		}

		_, e := rc.Do("EXEC")
		checkerror(e)

		// [vi.3] busted the cap?
		done := checkcap(searchkey, clientnumber, rc)
		if done {
			// trigger the break in the outer loop
			rcdel(rc, searchkey)
		}
	}
}

func checkcap(searchkey string, client int, rc redis.Conn) bool {
	resultkey := searchkey + "_results"
	hitcount, e := redis.Int64(rc.Do("SCARD", resultkey))
	checkerror(e)

	k := searchkey + "_hitcount"
	_, ee := rc.Do("SET", k, hitcount)
	checkerror(ee)
	msg(fmt.Sprintf("grabber #%d reports that the hitcount is %d", client, hitcount), 3)
	if hitcount >= cfg.MaxHits {
		// trigger the break in the outer loop
		return true
	} else {
		return false
	}
}

func findtherows(thequery string, thecaller string, searchkey string, clientnumber int, rc redis.Conn, dbpool *pgxpool.Pool) pgx.Rows {
	// called by both grabber() and HipparchiaBagger()

	// [ii] update the polling data
	if thecaller != "bagger" {
		remain, err := redis.Int64(rc.Do("SCARD", searchkey))
		checkerror(err)

		k := fmt.Sprintf("%s_remaining", searchkey)
		_, e := rc.Do("SET", k, remain)
		checkerror(e)
		msg(fmt.Sprintf("%s #%d says that %d items remain", thecaller, clientnumber, remain), 3)
	}

	// [iii] decode the query
	var prq PrerolledQuery
	err := json.Unmarshal([]byte(thequery), &prq)
	checkerror(err)

	// [iv] build a temp table if needed
	if prq.TempTable != "" {
		_, err := dbpool.Exec(context.Background(), prq.TempTable)
		checkerror(err)
	}

	// [v] execute the main query
	var foundrows pgx.Rows
	if prq.PsqlData != "" {
		foundrows, err = dbpool.Query(context.Background(), prq.PsqlQuery, prq.PsqlData)
		checkerror(err)
	} else {
		foundrows, err = dbpool.Query(context.Background(), prq.PsqlQuery)
		checkerror(err)
	}
	return foundrows
}

func recordinitialsizeofworkpile(k string) {
	rc := grabredisconnection()

	defer func(rc redis.Conn) {
		err := rc.Close()
		checkerror(err)
	}(rc)

	remain, err := redis.Int64(rc.Do("SCARD", k))
	checkerror(err)
	kk := fmt.Sprintf("%s_poolofwork", k)
	_, e := rc.Do("SET", kk, remain)
	checkerror(e)

	msg(fmt.Sprintf("recordinitialsizeofworkpile(): initial size of workpile for '%s' is %d", k+"_poolofwork", remain), 2)
}

func fetchfinalnumberofresults(k string) int64 {
	rc := grabredisconnection()

	defer func(rc redis.Conn) {
		err := rc.Close()
		checkerror(err)
	}(rc)

	kk := fmt.Sprintf("%s_results", k)
	hits, err := redis.Int64(rc.Do("SCARD", kk))
	checkerror(err)
	return hits
}
