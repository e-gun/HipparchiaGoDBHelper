//    HipparchiaGoDBHelper: search, vector and websocket helper app and functions for HipparchiaServer
//    Copyright: E Gunderson 2016-21
//    License: GNU GENERAL PUBLIC LICENSE 3
//        (see LICENSE in the top level directory of the distribution)

// HipparchiaGoVectorHelper and HipparchiaGoGrabberModule and HipparchiaGoWebSocketApp have been made to converge:
// the vector code needed to be able to search; and maybe there is no need for 2 or 3 binaries
//
// [I] the GRABBER is supposed to be pointedly basic
//
// [a] it looks to redis for a pile of SQL queries that were pre-rolled
// [b] it asks postgres to execute these queries
// [c] it stores the results on redis
// [d] it also updates the redis progress poll data relative to this search
//

// [II] VECTOR PREP builds bags for modeling; to do this you need to...
//
// [a] grab db lines that are relevant to the search
// [b] turn them into a unified text block
// [c] do some preliminary cleanups
// [d] break the text into sentences and assemble []SentenceWithLocus (NB: these are "unlemmatized bags of words")
// [e] figure out all of the words used in the passage
// [f] find all of the parsing info relative to these words
// [g] figure out which headwords to associate with the collection of words
// [h] build the lemmatized bags of words ('unlemmatized' can skip [f] and [g]...)
// [i] store the bags
//
// once you reach this point python can fetch the bags and then run "Word2Vec(bags, parameters, ...)"
//

//  [III] WEBSOCKETS broadcasts search information for web page updates
//
//	[a] it launches and starts listening on a port
//	[b] it waits to receive a websocket message: this is a search key ID (e.g., '2f81c630')
//	[c] it then looks inside of redis for the relevant polling data associated with that search ID
//	[d] it parses, packages (as JSON), and then redistributes this information back over the websocket
//	[e] when the poll disappears from redis, the messages stop broadcasting
//

// toggle the package name to shift between cli and module builds: main or hipparchiagolangsearching
package main

import (
	"C"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/jackc/pgx/v4/pgxpool"
	"os"
	"time"
)

const (
	redisexpiration = 5 * time.Minute
	myname          = "Hipparchia Golang Helper"
	shortname       = "HGH"
	version         = "1.0.12"
	tesquery        = "SELECT * FROM %s WHERE index BETWEEN %d and %d"
	testdb          = "lt0448"
	teststart       = 1
	testend         = 26
	linelength      = 72
	pollinginterval = 333 * time.Millisecond
	maxtrials       = 3
)

//
// see THEGRABBER.GO and THEVECTORS.GO for the two core functions of HipparchiaGoDBHelper
//

func main() {
	versioninfo := fmt.Sprintf("%s CLI Debugging Interface (v.%s)", myname, version)

	// WARNING: a password is going to be hard-coded into the binary. It is easy to use the binary in HipparchiaServer
	// without providing valid credentials to the binary, but if you do you must pass them and your credentials will be
	// visible to a "ps aux | grep ..."; but a hard-coded binary is not so good if you are going to share it...

	const (
		RP  = `{"Addr": "localhost:6379", "Password": "", "DB": 0}`
		PSQ = `{"Host": "localhost", "Port": 5432, "User": "hippa_wr", "Pass": "", "DBName": "hipparchiaDB"}`
	)

	var k string
	var c int64
	var g int
	var l int
	var r string
	var p string

	flag.StringVar(&k, "k", "go", "[searches] redis key to use")
	flag.Int64Var(&c, "c", 200, "[searches] max hit count")
	flag.IntVar(&g, "t", 5, "[common] number of goroutines to dispatch")
	flag.IntVar(&l, "l", 1, "[common] logging level: 0 is silent; 5 is very noisy")
	flag.StringVar(&r, "r", RP, "[common] redis logon information (as a JSON string)")
	flag.StringVar(&p, "p", PSQ, "[common] psql logon information (as a JSON string)")

	// vector flags

	var dbdb string
	var dbdbs int
	var dbdbe int
	var b string

	flag.StringVar(&b, "svb", "winnertakesall", "[vectors] the bagging method: choices are alternates, flat, unlemmatized, winnertakesall")
	flag.StringVar(&dbdb, "svdb", testdb, "[vectors][for manual debugging] db to grab from")
	flag.IntVar(&dbdbs, "svs", teststart, "[vectors][for manual debugging] first line to grab")
	flag.IntVar(&dbdbe, "sve", testend, "[vectors][for manual debugging] last line to grab")
	sv := flag.Bool("sv", false, "[vectors] assert that this is a vectorizing run")

	// websocket flags

	var wsp int
	var wsf int
	var wss int

	ws := flag.Bool("ws", false, "[websockets] assert that you are requesting the websocket server")
	flag.IntVar(&wsp, "wsp", 5010, "[websockets] port on which to open the websocket server")
	flag.IntVar(&wsf, "wsf", 3, "[websockets] fail threshold before messages stop being sent")
	flag.IntVar(&wss, "wss", 0, "[websockets] save the polls instead of deleting them: 0 is no; 1 is yes")

	v := flag.Bool("v", false, "[common] print version and exit")
	flag.Parse()

	if *v {
		fmt.Println(versioninfo)
		os.Exit(1)
	}

	versioninfo = versioninfo + fmt.Sprintf(" [loglevel=%d]", l)

	if l > 5 {
		l = 5
	}

	if l < 0 {
		l = 0
	}

	logiflogging(versioninfo, l, 1)

	rl := decoderedislogin([]byte(r))
	po := decodepsqllogin([]byte(p))

	var o string
	var t int64
	var x string

	if *sv {
		// vectors
		o = HipparchiaBagger(k, b, g, dbdb, dbdbs, dbdbe, l, rl, po)
		x = "bags"
		t = -1
	} else if *ws {
		// websockets
		StartHipparchiaPollWebsocket(wsp, l, wsf, wss, rl)
	} else {
		// searcher
		o = HipparchiaGolangSearcher(k, c, g, l, rl, po)
		t = fetchfinalnumberofresults(k, rl)
		x = "hits"
	}

	// DO NOT comment out the fmt.Printf(): the resultkey is parsed by HipparchiaServer when GOLANGLOADING = 'cli'
	// sharedlibraryclisearcher(): "resultrediskey = resultrediskey.split()[-1]"
	if t > -1 {
		fmt.Printf("%d %s have been stored at %s", t, x, o)
	} else {
		fmt.Printf("%s have been stored at %s", x, o)
	}
}

//
// PYTHON MODULE AUTHENTICATION
//

//NewRedisLogin: Generate new redis credentials (for module use only)
func NewRedisLogin(ad string, pw string, db int) *RedisLogin {
	// this is code that the python module version will use for authenticating
	return &RedisLogin{
		Addr:     ad,
		Password: pw,
		DB:       db,
	}
}

//NewPostgresLogin: Generate new postgres credentials (for module use only)
func NewPostgresLogin(ho string, po int, us string, pw string, db string) *PostgresLogin {
	// this is code that the python module version will use for authenticating
	return &PostgresLogin{
		Host:   ho,
		Port:   po,
		User:   us,
		Pass:   pw,
		DBName: db,
	}
}

//
// GENERAL AUTHENTICATION
//

func decoderedislogin(redislogininfo []byte) RedisLogin {
	var rl RedisLogin
	err := json.Unmarshal(redislogininfo, &rl)
	if err != nil {
		fmt.Println(fmt.Sprintf("CANNOT PARSE YOUR REDIS LOGIN CREDENTIALS AS JSON [%s v.%s] ", myname, version))
		panic(err)
	}
	return rl
}

func decodepsqllogin(psqllogininfo []byte) PostgresLogin {
	var ps PostgresLogin
	err := json.Unmarshal(psqllogininfo, &ps)
	if err != nil {
		fmt.Println(fmt.Sprintf("CANNOT PARSE YOUR POSTGRES LOGIN CREDENTIALS AS JSON [%s v.%s] ", myname, version))
		panic(err)
	}
	return ps
}

//
// DBCONNECTIONS
//

func grabredisconnection(r RedisLogin) *redis.Client {
	redisclient := redis.NewClient(&redis.Options{Addr: r.Addr, Password: r.Password, DB: r.DB})
	_, err := redisclient.Ping().Result()
	checkerror(err)
	return redisclient
}

func grabpgsqlconnection(p PostgresLogin, goroutines int, loglevel int) *pgxpool.Pool {

	url := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", p.User, p.Pass, p.Host, p.Port, p.DBName)

	config, oops := pgxpool.ParseConfig(url)
	if oops != nil {
		logiflogging(fmt.Sprintf("Could not execute pgxpool.ParseConfig(url) via %s", url), loglevel, 0)
		panic(oops)
	}

	config.ConnConfig.PreferSimpleProtocol = true
	config.MaxConns = int32((goroutines + 2) * 2)
	config.MinConns = int32(goroutines + 2)

	// the boring way if you don't want to go via pgxpool.ParseConfig(url)
	// dbpool, err := pgxpool.Connect(context.Background(), url)
	dbpool, err := pgxpool.ConnectConfig(context.Background(), config)

	if err != nil {
		logiflogging(fmt.Sprintf("Could not connect to PostgreSQL via %s", url), loglevel, 0)
		panic(err)
	}

	logiflogging(fmt.Sprintf("Connected to %s on PostgreSQL", p.DBName), loglevel, 2)

	return dbpool
}

//
// DEBUGGING
//

func checkerror(err error) {
	if err != nil {
		fmt.Println(fmt.Sprintf("UNRECOVERABLE ERROR: PLEASE TAKE NOTE OF THE FOLLOWING PANIC MESSAGE [%s v.%s]", myname, version))
		panic(err)
	}
}

func logiflogging(message string, loglevel int, threshold int) {
	if loglevel >= threshold {
		message = fmt.Sprintf("[%s] %s", shortname, message)
		fmt.Println(message)
	}
}
