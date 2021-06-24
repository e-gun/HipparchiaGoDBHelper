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
	// "encoding/json"
	"fmt"
	"github.com/bytedance/sonic"
	"github.com/go-redis/redis"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"runtime"
	"sync"
)

//HipparchiaGolangSearcher: Execute a series of SQL queries stored in redis by dispatching a collection of goroutines
func HipparchiaGolangSearcher(thekey string, hitcap int64, workercount int, loglevel int, rl RedisLogin, pl PostgresLogin) string {
	// this is the code that the python module version is calling instead of main()
	logiflogging(fmt.Sprintf("Searcher Module Launched"), loglevel, 1)

	runtime.GOMAXPROCS(workercount + 1)

	recordinitialsizeofworkpile(thekey, loglevel, rl)

	var awaiting sync.WaitGroup

	for i := 0; i < workercount; i++ {
		awaiting.Add(1)
		go grabber(i, hitcap, thekey, loglevel, rl, pl, &awaiting)
	}

	awaiting.Wait()

	resultkey := thekey + "_results"
	return resultkey
}

func grabber(clientnumber int, hitcap int64, searchkey string, ll int, rl RedisLogin, pl PostgresLogin, awaiting *sync.WaitGroup) {
	// this is where all of the work happens
	defer awaiting.Done()
	logiflogging(fmt.Sprintf("Hello from grabber %d", clientnumber), ll, 3)

	rc := grabredisconnection(rl)
	defer rc.Close()

	dbpool := grabpgsqlconnection(pl, 1, ll)
	defer dbpool.Close()

	resultkey := searchkey + "_results"

	for {
		// [i] get a pre-rolled query or break the loop
		thequery, err := rc.SPop(searchkey).Result()
		if err != nil {
			break
		}

		// [ii] - [v] inside findtherows() because its code is common with HipparchiaBagger's needs
		foundrows := findtherows(thequery, "grabber", searchkey, clientnumber, ll, rc, dbpool)

		// [vi] iterate through the finds
		defer foundrows.Close()
		for foundrows.Next() {
			// [vi.1] convert the find to a DbWorkline
			var thehit DbWorkline
			err = foundrows.Scan(&thehit.WkUID, &thehit.TbIndex, &thehit.Lvl5Value, &thehit.Lvl4Value, &thehit.Lvl3Value,
				&thehit.Lvl2Value, &thehit.Lvl1Value, &thehit.Lvl0Value, &thehit.MarkedUp, &thehit.Accented,
				&thehit.Stripped, &thehit.Hypenated, &thehit.Annotations)
			checkerror(err)

			// [vi.2] if you have not hit the cap on finds, store the result in 'querykey_results'
			// also update the polling hitcount key
			hitcount, err := rc.SCard(resultkey).Result()
			checkerror(err)
			rc.Set(searchkey+"_hitcount", hitcount, redisexpiration)
			logiflogging(fmt.Sprintf("grabber #%d reports that the hitcount is %d", clientnumber, hitcount), ll, 3)

			if hitcount >= hitcap {
				// trigger the break in the outer loop
				rc.Del(searchkey)
				foundrows.Close()
			} else {
				jsonhit, err := sonic.Marshal(thehit)
				checkerror(err)
				rc.SAdd(resultkey, jsonhit)
				logiflogging(fmt.Sprintf("grabber #%d added a result to %s: %s.%d", clientnumber, resultkey, thehit.WkUID, thehit.TbIndex), ll, 4)
			}
		}
	}
}

func findtherows(thequery string, thecaller string, searchkey string, clientnumber int, ll int, rc *redis.Client, dbpool *pgxpool.Pool) pgx.Rows {
	// called by both grabber() and HipparchiaBagger()

	// [ii] update the polling data
	if thecaller != "bagger" {
		remain, err := rc.SCard(searchkey).Result()
		checkerror(err)
		rc.Set(searchkey+"_remaining", remain, redisexpiration)
		logiflogging(fmt.Sprintf("%s #%d says that %d items remain", thecaller, clientnumber, remain), ll, 3)
	}

	// [iii] decode the query
	var prq PrerolledQuery
	err := sonic.Unmarshal([]byte(thequery), &prq)
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

func recordinitialsizeofworkpile(k string, loglevel int, rl RedisLogin) {
	rc := grabredisconnection(rl)
	defer rc.Close()
	remain, err := rc.SCard(k).Result()
	checkerror(err)
	rc.Set(k+"_poolofwork", remain, redisexpiration)
	logiflogging(fmt.Sprintf("recordinitialsizeofworkpile(): initial size of workpile for '%s' is %d", k+"_poolofwork", remain), loglevel, 2)
}

func fetchfinalnumberofresults(k string, rl RedisLogin) int64 {
	rc := grabredisconnection(rl)
	defer rc.Close()
	hits, err := rc.SCard(k + "_results").Result()
	checkerror(err)
	return hits
}
