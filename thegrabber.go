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

//HipparchiaGolangSearcher: Execute a series of SQL queries stored in redis by dispatching a collection of goroutines
func HipparchiaGolangSearcher(thekey string, hitcap int64, workercount int, ll int, rl RedisLogin, pl PostgresLogin) string {
	// this is the code that the python module version is calling instead of main()
	// that also means that the module needs to be able to set the logging level
	logginglevel = ll
	msg(fmt.Sprintf("Searcher Launched"), 1)

	runtime.GOMAXPROCS(workercount + 1)

	recordinitialsizeofworkpile(thekey, rl)

	var awaiting sync.WaitGroup

	for i := 0; i < workercount; i++ {
		awaiting.Add(1)
		go grabber(i, hitcap, thekey, rl, pl, &awaiting)
	}

	awaiting.Wait()

	resultkey := thekey + "_results"
	return resultkey
}

func grabber(clientnumber int, hitcap int64, searchkey string, rl RedisLogin, pl PostgresLogin, awaiting *sync.WaitGroup) {
	// this is where all of the work happens
	defer awaiting.Done()
	msg(fmt.Sprintf("Hello from grabber %d", clientnumber), 3)

	rc := grabredisconnection(rl)
	defer func(rc redis.Conn) {
		err := rc.Close()
		checkerror(err)
	}(rc)

	dbpool := grabpgsqlconnection(pl, 1)
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
		// if you search for "y" near "x", during the first search iteration you will see:
		// 	[debugging] [HGH] grabber #0 reports that the hitcount is 19023 [debugging]
		// subsequently you will see:
		//	[debugging] [HGH] grabber #2 reports that the hitcount is 243 [debugging]
		//	[debugging] [HGH] grabber #0 reports that the hitcount is 244 [debugging]
		//	[debugging] [HGH] grabber #3 reports that the hitcount is 351 [debugging]
		//	[debugging] [HGH] grabber #1 reports that the hitcount is 351 [debugging]
		// and yet only 200 items will come back at you if 200 is your cap
		// faster to test only after you finish each query
		// can over-load redis because HipparchaServer should only display hitcap results no matter how many you push

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
		done := checkcap(searchkey, hitcap, clientnumber, rc)
		if done {
			// trigger the break in the outer loop
			rcdel(rc, searchkey)
		}
	}
}

func checkcap(searchkey string, cap int64, client int, rc redis.Conn) bool {
	resultkey := searchkey + "_results"
	hitcount, e := redis.Int64(rc.Do("SCARD", resultkey))
	checkerror(e)

	k := searchkey + "_hitcount"
	_, ee := rc.Do("SET", k, hitcount)
	checkerror(ee)
	msg(fmt.Sprintf("grabber #%d reports that the hitcount is %d", client, hitcount), 3)
	if hitcount >= cap {
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

func recordinitialsizeofworkpile(k string, rl RedisLogin) {
	rc := grabredisconnection(rl)

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

func fetchfinalnumberofresults(k string, rl RedisLogin) int64 {
	rc := grabredisconnection(rl)

	defer func(rc redis.Conn) {
		err := rc.Close()
		checkerror(err)
	}(rc)

	kk := fmt.Sprintf("%s_results", k)
	hits, err := redis.Int64(rc.Do("SCARD", kk))
	checkerror(err)
	return hits
}
