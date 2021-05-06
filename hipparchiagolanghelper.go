//    HipparchiaGoDBHelper: search and vector helper app and functions for HipparchiaServer
//    Copyright: E Gunderson 2016-21
//    License: GNU GENERAL PUBLIC LICENSE 3
//        (see LICENSE in the top level directory of the distribution)

// HipparchiaGoVectorHelper and HipparchiaGoGrabberModule have been made to converge:
// the vector code needed to be able to search
//
// the GRABBER is supposed to be pointedly basic
// [a] it looks to redis for a pile of SQL queries that were pre-rolled
// [b] it asks postgres to execute these queries
// [c] it stores the results on redis
// [d] it also updates the redis progress poll data relative to this search
//

// VECTOR PREP builds bags for modeling; to do this you need to...

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

// toggle the package name to shift between cli and module builds: main or hipparchiagolangsearching
package main

import (
	"C"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"os"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	redisexpiration = 5 * time.Minute
	myname          = "Hipparchia Golang Helper"
	shortname       = "HGH"
	version         = "0.1.0"
	tesquery        = "SELECT * FROM %s WHERE index BETWEEN %d and %d"
	testdb          = "lt0448"
	teststart       = 1
	testend         = 26
	linelength      = 72
)

type PrerolledQuery struct {
	TempTable string
	PsqlQuery string
	PsqlData  string
}

type DbWorkline struct {
	WkUID       string
	TbIndex     int
	Lvl5Value   string
	Lvl4Value   string
	Lvl3Value   string
	Lvl2Value   string
	Lvl1Value   string
	Lvl0Value   string
	MarkedUp    string
	Accented    string
	Stripped    string
	Hypenated   string
	Annotations string
}

type RedisLogin struct {
	Addr     string
	Password string
	DB       int
}

type PostgresLogin struct {
	Host   string
	Port   int
	User   string
	Pass   string
	DBName string
}

// for vectors

// https://golangbyexample.com/sort-custom-struct-collection-golang/
type WeightedHeadword struct {
	Word  string
	Count int
}

type WHWList []WeightedHeadword

func (w WHWList) Len() int {
	return len(w)
}

func (w WHWList) Less(i, j int) bool {
	return w[i].Count > w[j].Count
}

func (w WHWList) Swap(i, j int) {
	w[i], w[j] = w[j], w[i]
}

type SentenceWithLocus struct {
	Loc  string
	Sent string
}

type DbMorphology struct {
	Observed   string
	Xrefs      string
	PefixXrefs string
	RawPossib  string
	UniqPossib map[string]bool
}

type MorphPossibility struct {
	Observed string
	Number   string
	Entry    string
	Xref     string
	TrAnal   string
}

func main() {
	versioninfo := fmt.Sprintf("%s CLI Debugging Interface (v.%s)", myname, version)

	var k string
	var c int64
	var g int
	var l int
	var r string
	var p string

	var dbdb string
	var dbdbs int
	var dbdbe int
	var b string

	// WARNING: a password is going to be hard-coded into the binary. It is easy to use the binary in HipparchiaServer
	// without providing valid credentials to the binary, but if you do you must pass them and your credentials will be
	// visible to a "ps aux | grep ..."; but a hard-coded binary is not so good if you are going to share it...

	const (
		RP  = `{"Addr": "localhost:6379", "Password": "", "DB": 0}`
		PSQ = `{"Host": "localhost", "Port": 5432, "User": "hippa_wr", "Pass": "", "DBName": "hipparchiaDB"}`
	)

	flag.StringVar(&k, "k", "go", "redis key to use")
	flag.Int64Var(&c, "c", 200, "max hit count")
	flag.IntVar(&g, "t", 5, "number of goroutines to dispatch")
	flag.IntVar(&l, "l", 1, "logging level: 0 is silent; 5 is very noisy")
	flag.StringVar(&r, "r", RP, "redis logon information (as a JSON string)")
	flag.StringVar(&p, "p", PSQ, "psql logon information (as a JSON string)")

	// vector flags
	flag.StringVar(&b, "svb", "winnertakesall", "[vectors] the bagging method: choices are alternates, flat, unlemmatized, winnertakesall")
	flag.StringVar(&dbdb, "svdb", testdb, "[vectors][for manual debugging] db to grab from")
	flag.IntVar(&dbdbs, "svs", teststart, "[vectors][for manual debugging] first line to grab")
	flag.IntVar(&dbdbe, "sve", testend, "[vectors][for manual debugging] last line to grab")
	sv := flag.Bool("sv", false, "[vectors] assert that this is a vectorizing run")

	v := flag.Bool("v", false, "print version and exit")
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

	if !*sv {
		o = HipparchiaGolangSearcher(k, c, g, l, rl, po)
		t = fetchfinalnumberofresults(k, rl)
		x = "hits"
	} else {
		o = HipparchiaBagger(k, b, g, dbdb, dbdbs, dbdbe, l, rl, po)
		x = "bags"
		t = -1
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
// GOLANGGRABBER
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

//HipparchiaGolangSearcher: Execute a series of SQL queries stored in redis by dispatching a collection of goroutines
func HipparchiaGolangSearcher(searchkey string, hitcap int64, goroutines int, loglevel int, r RedisLogin, p PostgresLogin) string {
	// this is the code that the python module version is calling instead of main()
	runtime.GOMAXPROCS(goroutines)

	recordinitialsizeofworkpile(searchkey, loglevel, r)

	var awaiting sync.WaitGroup

	for i := 0; i < goroutines; i++ {
		awaiting.Add(1)
		go grabber(i, hitcap, searchkey, loglevel, r, p, &awaiting)
	}

	awaiting.Wait()

	resultkey := searchkey + "_results"
	return resultkey
}

func grabber(clientnumber int, hitcap int64, searchkey string, loglevel int, r RedisLogin, p PostgresLogin, awaiting *sync.WaitGroup) {
	// this is where all of the work happens
	defer awaiting.Done()
	logiflogging(fmt.Sprintf("Hello from grabber %d", clientnumber), loglevel, 3)

	redisclient := redis.NewClient(&redis.Options{Addr: r.Addr, Password: r.Password, DB: r.DB})
	_, err := redisclient.Ping().Result()
	checkerror(err)
	defer redisclient.Close()
	logiflogging(fmt.Sprintf("grabber #%d connected to redis", clientnumber), loglevel, 2)

	url := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", p.User, p.Pass, p.Host, p.Port, p.DBName)
	dbpool, err := pgxpool.Connect(context.Background(), url)
	if err != nil {
		logiflogging(fmt.Sprintf("Could not connect to PostgreSQL via %s", url), loglevel, 0)
		panic(err)
	}
	defer dbpool.Close()
	logiflogging(fmt.Sprintf("grabber #%d Connected to %s on PostgreSQL", clientnumber, p.DBName), loglevel, 2)

	resultkey := searchkey + "_results"

	for {
		// [a] get a query
		byteArray, err := redisclient.SPop(searchkey).Result()
		if err != nil {
			break
		}

		remain, err := redisclient.SCard(searchkey).Result()
		checkerror(err)
		logiflogging(fmt.Sprintf("grabber #%d says that %d items remain", clientnumber, remain), loglevel, 3)

		// [b] update the polling data
		redisclient.Set(searchkey+"_remaining", remain, redisexpiration)

		// [c] decode the query
		var prq PrerolledQuery
		err = json.Unmarshal([]byte(byteArray), &prq)
		checkerror(err)

		// [d] build a temp table if needed
		if prq.TempTable != "" {
			_, err := dbpool.Exec(context.Background(), prq.TempTable)
			checkerror(err)
		}

		// [e] execute the main query
		var foundrows pgx.Rows
		logiflogging(fmt.Sprintf("grabber #%d will ask: %s", clientnumber, prq.PsqlQuery), loglevel, 4)
		if prq.PsqlData != "" {
			foundrows, err = dbpool.Query(context.Background(), prq.PsqlQuery, prq.PsqlData)
			checkerror(err)
		} else {
			foundrows, err = dbpool.Query(context.Background(), prq.PsqlQuery)
			checkerror(err)
		}

		// [f] iterate through the finds
		defer foundrows.Close()
		for foundrows.Next() {
			// [f1] convert the find to a DbWorkline
			var thehit DbWorkline
			err = foundrows.Scan(&thehit.WkUID, &thehit.TbIndex, &thehit.Lvl5Value, &thehit.Lvl4Value, &thehit.Lvl3Value,
				&thehit.Lvl2Value, &thehit.Lvl1Value, &thehit.Lvl0Value, &thehit.MarkedUp, &thehit.Accented,
				&thehit.Stripped, &thehit.Hypenated, &thehit.Annotations)
			checkerror(err)
			// fmt.Println(thehit)

			// [f2] if you have not hit the cap on finds, store the result in 'querykey_results'
			// also update the polling hitcount key
			hitcount, err := redisclient.SCard(resultkey).Result()
			logiflogging(fmt.Sprintf("grabber #%d reports that the hitcount is %d", clientnumber, hitcount), loglevel, 3)
			redisclient.Set(searchkey+"_hitcount", hitcount, redisexpiration)
			checkerror(err)
			if hitcount >= hitcap {
				// trigger the break in the outer loop
				redisclient.Del(searchkey)
			} else {
				jsonhit, err := json.Marshal(thehit)
				checkerror(err)
				redisclient.SAdd(resultkey, jsonhit)
				logiflogging(fmt.Sprintf("grabber #%d added a result to %s: %s.%d", clientnumber, resultkey, thehit.WkUID, thehit.TbIndex), loglevel, 4)
			}
		}
	}
}

func recordinitialsizeofworkpile(k string, loglevel int, rl RedisLogin) {
	redisclient := redis.NewClient(&redis.Options{Addr: rl.Addr, Password: rl.Password, DB: rl.DB})
	defer redisclient.Close()
	remain, err := redisclient.SCard(k).Result()
	checkerror(err)
	redisclient.Set(k+"_poolofwork", remain, redisexpiration)
	logiflogging(fmt.Sprintf("recordinitialsizeofworkpile(): initial size of workpile for '%s' is %d", k+"_poolofwork", remain), loglevel, 2)
}

func fetchfinalnumberofresults(k string, rl RedisLogin) int64 {
	redisclient := redis.NewClient(&redis.Options{Addr: rl.Addr, Password: rl.Password, DB: rl.DB})
	defer redisclient.Close()
	hits, err := redisclient.SCard(k + "_results").Result()
	checkerror(err)
	return hits
}

//
// SEMANTIC VECTOR HELPER
//

//HipparchiaBagger: Take a key; grab lines; bag them; store them
func HipparchiaBagger(searchkey string, baggingmethod string, goroutines int, thedb string, thestart int, theend int, loglevel int, r RedisLogin, p PostgresLogin) string {
	start := time.Now()
	logiflogging(fmt.Sprintf("Seeking to build *%s* bags of words", baggingmethod), loglevel, 2)

	redisclient := redis.NewClient(&redis.Options{Addr: r.Addr, Password: r.Password, DB: r.DB})
	_, err := redisclient.Ping().Result()
	checkerror(err)
	defer redisclient.Close()
	logiflogging(fmt.Sprintf("Connected to redis"), loglevel, 2)

	url := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", p.User, p.Pass, p.Host, p.Port, p.DBName)
	dbpool, err := pgxpool.Connect(context.Background(), url)
	if err != nil {
		logiflogging(fmt.Sprintf("Could not connect to PostgreSQL via %s", url), loglevel, 0)
		panic(err)
	}

	defer dbpool.Close()
	logiflogging(fmt.Sprintf("Connected to %s on PostgreSQL", p.DBName), loglevel, 2)

	// [a] grab the db lines
	// we should really be pulling these out of redis...

	// we do this by copying the code inside of grabber but just cut out the storage bits
	remain, err := redisclient.SCard(searchkey).Result()
	checkerror(err)
	redisclient.Set(searchkey+"_poolofwork", remain, redisexpiration)

	dblines := make(map[int]DbWorkline)

	if searchkey == "" {
		logiflogging(fmt.Sprintf("No redis key; gathering lines with a direct CLI PostgreSQL query", p.DBName), loglevel, 1)
		dblines = fetchdblinesdirectly(thedb, thestart, theend, dbpool)
	} else {
		count := 0
		for {
			// [a] get a query
			byteArray, err := redisclient.SPop(searchkey).Result()
			if err != nil {
				break
			}

			remain, err := redisclient.SCard(searchkey).Result()
			checkerror(err)
			logiflogging(fmt.Sprintf("bagger says that %d locations still need grabbing", remain), loglevel, 3)

			// [b] update the polling data
			redisclient.Set(searchkey+"_remaining", remain, redisexpiration)

			// [c] decode the query
			var prq PrerolledQuery
			err = json.Unmarshal([]byte(byteArray), &prq)
			checkerror(err)

			// [d] build a temp table if needed
			if prq.TempTable != "" {
				_, err := dbpool.Exec(context.Background(), prq.TempTable)
				checkerror(err)
			}

			// [e] execute the main query
			var foundrows pgx.Rows
			logiflogging(fmt.Sprintf("bagger will ask: %s", prq.PsqlQuery), loglevel, 5)
			if prq.PsqlData != "" {
				foundrows, err = dbpool.Query(context.Background(), prq.PsqlQuery, prq.PsqlData)
				checkerror(err)
			} else {
				foundrows, err = dbpool.Query(context.Background(), prq.PsqlQuery)
				checkerror(err)
			}

			// [f] iterate through the finds
			defer foundrows.Close()
			for foundrows.Next() {
				count += 1
				// [f1] convert the find to a DbWorkline
				var thehit DbWorkline
				err = foundrows.Scan(&thehit.WkUID, &thehit.TbIndex, &thehit.Lvl5Value, &thehit.Lvl4Value, &thehit.Lvl3Value,
					&thehit.Lvl2Value, &thehit.Lvl1Value, &thehit.Lvl0Value, &thehit.MarkedUp, &thehit.Accented,
					&thehit.Stripped, &thehit.Hypenated, &thehit.Annotations)
				checkerror(err)
				dblines[count] = thehit
			}
		}
	}

	// turn of progress logging
	redisclient.Set(searchkey+"_poolofwork", -1, redisexpiration)
	redisclient.Set(searchkey+"_hitcount", 0, redisexpiration)

	// [b] turn them into a unified text block

	// string addition will us a huge amount of time: 120s to concatinate Cicero: txt = txt + newtxt...
	// with strings.Builder we only need .1s to build the text...
	var sb strings.Builder
	preallocate := linelength * len(dblines) // NB: a long line has 60 chars
	sb.Grow(preallocate)

	for i := 0; i < len(dblines); i++ {
		newtxt := fmt.Sprintf("⊏line/%s/%d⊐%s ", dblines[i].WkUID, dblines[i].TbIndex, dblines[i].MarkedUp)
		sb.WriteString(newtxt)
	}

	txt := sb.String()
	sb.Reset()

	logiflogging(fmt.Sprintf("unified text block built [B: %fs])", time.Now().Sub(start).Seconds()), loglevel, 4)
	// fmt.Println(fmt.Sprintf(txt))

	// [c] do some preliminary cleanups
	// parsevectorsentences()

	strip := []string{`&nbsp;`, `- `, `<.*?>`}
	txt = stripper(txt, strip)

	logiflogging(fmt.Sprintf("preliminary cleanups complete [C: %fs])", time.Now().Sub(start).Seconds()), loglevel, 4)

	// [d] break the text into sentences and assemble []SentenceWithLocus

	// split at more than just one kind of punctuation...
	terminations := []string{".", "?", "!", "·", ";"}
	s := recursivesplitter([]string{txt}, terminations, 0, len(terminations))

	// the vanilla version...
	// s := strings.Split(txt, ".")

	var sentences []SentenceWithLocus
	var first string
	var last string

	const tagger = `⊏(.*?)⊐`
	const notachar = `[^\sa-zα-ωἀἁἂἃἄἅἆἇᾀᾁᾂᾃᾄᾅᾆᾇᾲᾳᾴᾶᾷᾰᾱὰάἐἑἒἓἔἕὲέἰἱἲἳἴἵἶἷὶίῐῑῒΐῖῗὀὁὂὃὄὅόὸὐὑὒὓὔὕὖὗϋῠῡῢΰῦῧύὺᾐᾑᾒᾓᾔᾕᾖᾗῂῃῄῆῇἤἢἥἣὴήἠἡἦἧὠὡὢὣὤὥὦὧᾠᾡᾢᾣᾤᾥᾦᾧῲῳῴῶῷώὼ]`
	re := regexp.MustCompile(tagger)

	for i := 0; i < len(s); i++ {
		tags := re.FindAllStringSubmatch(s[i], -1)
		if len(tags) > 0 {
			first = tags[0][1]
			last = tags[len(tags)-1][1]
		} else {
			first = last
		}
		var sl SentenceWithLocus
		sl.Loc = first
		sl.Sent = strings.ToLower(s[i])
		sl.Sent = stripper(sl.Sent, []string{tagger, notachar})
		sentences = append(sentences, sl)
		// fmt.Println(fmt.Sprintf("[%d] %s", i, s[i]))
	}

	logiflogging(fmt.Sprintf("found %d sentences [D: %fs]", len(sentences), time.Now().Sub(start).Seconds()), loglevel, 2)

	// unlemmatized bags of words customers are in fact done right now
	if baggingmethod == "unlemmatized" {
		resultkey := storebagsofwords(searchkey, sentences, redisclient)
		// DO NO comment out the fmt.Printf(): the resultkey is parsed by HipparchiaServer
		// "resultrediskey = resultrediskey.split()[-1]"
		fmt.Println(fmt.Sprintf("%d %s bags of words stored at %s", len(sentences), baggingmethod, resultkey))
		os.Exit(0)
	}

	// ex sentence: {line/lt0448w001/22  Belgae ab extremis Galliae finibus oriuntur, pertinent ad inferiorem partem fluminis Rheni, spectant in septentrionem et orientem solem}

	//for i := 0; i < len(sentences); i++ {
	//	fmt.Println(sentences[i])
	//}

	// [e] figure out all of the words used in the passage

	// buildnnvectorspace()

	// generate a "set" via make(map[string]bool)
	allwords := make(map[string]bool, len(sentences))
	for i := 0; i < len(sentences); i++ {
		ww := strings.Split(sentences[i].Sent, " ")
		for j := 0; j < len(ww); j++ {
			allwords[ww[j]] = true
		}
	}

	logiflogging(fmt.Sprintf("found %d distinct words [E: %fs]", len(allwords), time.Now().Sub(start).Seconds()), loglevel, 4)

	logiflogging(fmt.Sprintf("found %d distinct words [E: %fs]", len(allwords), time.Now().Sub(start).Seconds()), loglevel, 4)

	//for w := range allwords {
	//	fmt.Printf("%s ", w)
	//}

	// [f] find all of the parsing info relative to these words

	// getrequiredmorphobjects(): a candidate for goroutines
	// lookformorphologymatches()

	// can only send the keys to getrequiredmorphobjects(); so we need to demap things
	keys := make([]string, 0, len(allwords))
	for k := range allwords {
		keys = append(keys, k)
	}

	var mo map[string]DbMorphology
	mo = getrequiredmorphobjects(keys, dbpool)
	logiflogging(fmt.Sprintf("Got morphology [F: %fs]", time.Now().Sub(start).Seconds()), loglevel, 3)

	//for k := range mo {
	//	fmt.Println(fmt.Sprintf("%s", mo[k].Observed))
	//}

	// [g] figure out which headwords to associate with the collection of words

	// see convertmophdicttodict()
	// a set of sets
	//	key = word-in-use
	//	value = { maybeA, maybeB, maybeC}
	// {'θεῶν': {'θεόϲ', 'θέα', 'θεάω', 'θεά'}, 'πώ': {'πω'}, 'πολλά': {'πολύϲ'}, 'πατήρ': {'πατήρ'}, ... }

	morphdict := make(map[string]map[string]bool)
	for m := range mo {
		// unpack the unique possibilities
		pp := make([]MorphPossibility, 0, len(mo[m].UniqPossib))
		for k := range mo[m].UniqPossib {
			pp = append(pp, getpossiblemorph(m, k))
		}
		// add them to the collection of possibilities or generate a new slot for them in the collection
		for i := 0; i < len(pp); i++ {
			if _, t := morphdict[pp[i].Observed]; t {
				// X is already present in 'morphdict'; need to add this headword to the set of headwords
				morphdict[pp[i].Observed][pp[i].Entry] = true
			} else {
				morphdict[pp[i].Observed] = make(map[string]bool)
				morphdict[pp[i].Observed][pp[i].Entry] = true
			}
		}
	}

	// no need for the "bool" any longer; demap things
	flatdict := make(map[string][]string, len(morphdict))
	for i := range morphdict {
		keys := make([]string, 0, len(morphdict[i]))
		for k := range morphdict[i] {
			keys = append(keys, k)
		}
		flatdict[i] = keys
	}

	//for q := range flatdict {
	//	fmt.Println(fmt.Sprintf("%s:\n\t%s", q, flatdict[q]))
	//}

	switch baggingmethod {
	case "flat":
		sentences = buildflatbagsofwords(sentences, flatdict)
	case "alternates":
		sentences = buildcompositebagsofwords(sentences, flatdict)
	case "winnertakesall":
		sentences = buildwinnertakesallbagsofwords(sentences, flatdict, dbpool)
	default:
		logiflogging(fmt.Sprintf("unknown bagging method '%s'; storing unlemmatized bags", baggingmethod), loglevel, 2)
	}

	resultkey := storebagsofwords(searchkey, sentences, redisclient)
	if len(sentences) > 1 {
		logiflogging(fmt.Sprintf("contents of bag[0]: %s", sentences[0].Sent), loglevel, 3)
		logiflogging(fmt.Sprintf("contents of bag[1]: %s", sentences[1].Sent), loglevel, 3)
	}
	logiflogging(fmt.Sprintf("Reached result @ %fs]", time.Now().Sub(start).Seconds()), loglevel, 1)
	return resultkey
}

func fetchdblinesdirectly(thedb string, thestart int, theend int, dbpool *pgxpool.Pool) map[int]DbWorkline {
	// No redis key; gathering lines with a direct PostgreSQL query
	dblines := make(map[int]DbWorkline)
	foundrows, err := dbpool.Query(context.Background(), fmt.Sprintf(tesquery, thedb, thestart, theend))
	checkerror(err)
	count := -1
	defer foundrows.Close()
	for foundrows.Next() {
		count += 1
		// [f1] convert the find to a DbWorkline
		var thehit DbWorkline
		err = foundrows.Scan(&thehit.TbIndex, &thehit.WkUID, &thehit.Lvl5Value, &thehit.Lvl4Value, &thehit.Lvl3Value,
			&thehit.Lvl2Value, &thehit.Lvl1Value, &thehit.Lvl0Value, &thehit.MarkedUp, &thehit.Accented,
			&thehit.Stripped, &thehit.Hypenated, &thehit.Annotations)
		checkerror(err)
		dblines[count] = thehit
	}
	return dblines
}

func stripper(item string, purge []string) string {
	// delete each in a list of items from a string
	for i := 0; i < len(purge); i++ {
		re := regexp.MustCompile(purge[i])
		item = re.ReplaceAllString(item, "")
	}
	return item
}

func storebagsofwords(searchkey string, bags []SentenceWithLocus, redisclient *redis.Client) string {
	kk := strings.Split(searchkey, "_")
	resultkey := kk[0] + "_vectorresults"
	for i := 0; i < len(bags); i++ {
		jsonhit, err := json.Marshal(bags[i])
		checkerror(err)
		redisclient.SAdd(resultkey, jsonhit)
	}
	return resultkey
}

func buildflatbagsofwords(bags []SentenceWithLocus, parsemap map[string][]string) []SentenceWithLocus {
	// turn a list of sentences into a list of list of headwords; here we put alternate possibilities next to one another:
	// flatbags: ϲυγγενεύϲ ϲυγγενήϲ
	// composite: ϲυγγενεύϲ·ϲυγγενήϲ
	// this is parallizable...
	for i := 0; i < len(bags); i++ {
		var newwords []string
		words := strings.Split(bags[i].Sent, " ")
		for j := 0; j < len(words); j++ {
			newwords = append(newwords, parsemap[words[j]]...)
		}
		bags[i].Sent = strings.Join(newwords, " ")
	}
	return bags
}

func buildcompositebagsofwords(bags []SentenceWithLocus, parsemap map[string][]string) []SentenceWithLocus {
	// turn a list of sentences into a list of list of headwords; here we put yoked alternate possibilities next to one another:
	// flatbags: ϲυγγενεύϲ ϲυγγενήϲ
	// composite: ϲυγγενεύϲ·ϲυγγενήϲ
	// this is parallizable...
	for i := 0; i < len(bags); i++ {
		var newwords []string
		words := strings.Split(bags[i].Sent, " ")
		for j := 0; j < len(words); j++ {
			comp := strings.Join(parsemap[words[j]], "·")
			newwords = append(newwords, comp)
		}
		bags[i].Sent = strings.Join(newwords, " ")
	}
	return bags
}

func buildwinnertakesallbagsofwords(bags []SentenceWithLocus, parsemap map[string][]string, dbpool *pgxpool.Pool) []SentenceWithLocus {
	// turn a list of sentences into a list of list of headwords; here we figure out which headword is the dominant homonym
	// then we just use that term; "esse" always comes from "sum" and never "edo", etc.

	// [a] figure out all headwords in use

	allheadwords := make(map[string]bool)
	for i := range parsemap {
		for j := range parsemap[i] {
			allheadwords[parsemap[i][j]] = true
		}
	}

	// [b] assign scores to each of them

	scoremap := fetchheadwordcounts(allheadwords, dbpool)

	// [c] run through the parsemap and kill off the losers

	newparsemap := make(map[string][]string)
	for i := range parsemap {
		var hwl WHWList
		for j := 0; j < len(parsemap[i]); j++ {
			var thishw WeightedHeadword
			thishw.Word = parsemap[i][j]
			thishw.Count = scoremap[thishw.Word]
			hwl = append(hwl, thishw)
		}
		sort.Sort(hwl)
		newparsemap[i] = make([]string, 0, 1)
		newparsemap[i] = append(newparsemap[i], hwl[0].Word)
	}

	// [d] now you can just buildflatbagsofwords() with the new pruned parsemap

	bags = buildflatbagsofwords(bags, newparsemap)

	return bags
}

func looptogetrequiredmorphobjects(wordlist []string, dbpool *pgxpool.Pool) map[string]DbMorphology {
	// the slow but sure way...

	latintest := regexp.MustCompile(`[a-z]`)
	qtemplate := "SELECT observed_form, xrefs, prefixrefs, possible_dictionary_forms FROM %s_morphology WHERE observed_form = $1"
	var q string

	foundmorph := make(map[string]DbMorphology)

	for i := 0; i < len(wordlist); i++ {
		if latintest.MatchString(wordlist[i]) {
			q = fmt.Sprintf(qtemplate, "latin")
		} else {
			q = fmt.Sprintf(qtemplate, "greek")
		}
		foundrows, err := dbpool.Query(context.Background(), q, wordlist[i])
		checkerror(err)
		defer foundrows.Close()
		for foundrows.Next() {
			var thehit DbMorphology
			err = foundrows.Scan(&thehit.Observed, &thehit.Xrefs, &thehit.PefixXrefs, &thehit.RawPossib)
			checkerror(err)
			thehit.UniqPossib = make(map[string]bool)
			if _, t := foundmorph[thehit.Observed]; t {
				// logiflogging(fmt.Sprintf("%s is already present; need to append these possibilites to the other possibilities", thehit.Observed), 0, 0)
				newpos := updatesetofpossibilities(thehit.RawPossib, thehit.UniqPossib)
				for p := range newpos {
					if _, t := foundmorph[thehit.Observed].UniqPossib[p]; !t {
						// logiflogging(fmt.Sprintf("appending to %s: %s", thehit.Observed, p), 0, 0)
						foundmorph[thehit.Observed].UniqPossib[p] = true
					}
				}
			} else {
				thehit.UniqPossib = updatesetofpossibilities(thehit.RawPossib, thehit.UniqPossib)
				foundmorph[thehit.Observed] = thehit
			}
		}
	}
	logiflogging(fmt.Sprintf("foundmorph contains %d", len(foundmorph)), 0, 0)

	return foundmorph
}

func getrequiredmorphobjects(wordlist []string, dbpool *pgxpool.Pool) map[string]DbMorphology {
	// run arraytogetrequiredmorphobjects once for each language
	latintest := regexp.MustCompile(`[a-z]`)
	var latinwords []string
	var greekwords []string
	for i := 0; i < len(wordlist); i++ {
		if latintest.MatchString(wordlist[i]) {
			latinwords = append(latinwords, wordlist[i])
		} else {
			greekwords = append(greekwords, wordlist[i])
		}
	}

	lt := arraytogetrequiredmorphobjects(latinwords, "latin", dbpool)
	gk := arraytogetrequiredmorphobjects(greekwords, "greek", dbpool)

	mo := make(map[string]DbMorphology)
	for k, v := range gk {
		mo[k] = v
	}

	for k, v := range lt {
		mo[k] = v
	}

	logiflogging(fmt.Sprintf("foundmorph contains %d members", len(mo)), 0, 0)
	return mo
}

func arraytogetrequiredmorphobjects(wordlist []string, uselang string, dbpool *pgxpool.Pool) map[string]DbMorphology {
	// better to use an array, but pgx is balking if you try to pass the array as $1 at dbpool.Exec()
	// yet this seems perfectly // to fetchheadwordcounts()

	// hipparchiaDB=# CREATE TEMPORARY TABLE ttw AS SELECT words AS w FROM unnest(ARRAY['dolor', 'amor', 'lusus']) words;
	// hipparchiaDB=# SELECT observed_form, xrefs, prefixrefs, possible_dictionary_forms FROM latin_morphology
	//					WHERE EXISTS (SELECT 1 FROM ttw temptable WHERE temptable.w = latin_morphology.observed_form);
	tt := "CREATE TEMPORARY TABLE ttw_%s AS SELECT words AS w FROM unnest(ARRAY[%s]) words"
	qt := "SELECT observed_form, xrefs, prefixrefs, possible_dictionary_forms FROM %s_morphology WHERE EXISTS " +
		"(SELECT 1 FROM ttw_%s temptable WHERE temptable.w = %s_morphology.observed_form)"
	rndid := strings.Replace(uuid.New().String(), "-", "", -1)
	arr := strings.Join(wordlist, "', '")
	arr = "'" + arr + "'"
	tt = fmt.Sprintf(tt, rndid, arr)

	_, err := dbpool.Exec(context.Background(), tt)
	checkerror(err)

	foundrows, e := dbpool.Query(context.Background(), fmt.Sprintf(qt, uselang, rndid, uselang))
	checkerror(e)

	foundmorph := make(map[string]DbMorphology)
	defer foundrows.Close()
	count := 0
	for foundrows.Next() {
		count += 1
		var thehit DbMorphology
		err = foundrows.Scan(&thehit.Observed, &thehit.Xrefs, &thehit.PefixXrefs, &thehit.RawPossib)
		checkerror(err)
		thehit.UniqPossib = make(map[string]bool)
		if _, t := foundmorph[thehit.Observed]; t {
			newpos := updatesetofpossibilities(thehit.RawPossib, thehit.UniqPossib)
			for p := range newpos {
				if _, t := foundmorph[thehit.Observed].UniqPossib[p]; !t {
					foundmorph[thehit.Observed].UniqPossib[p] = true
				}
			}
		} else {
			thehit.UniqPossib = updatesetofpossibilities(thehit.RawPossib, thehit.UniqPossib)
			foundmorph[thehit.Observed] = thehit
		}
	}

	return foundmorph
}

func updatesetofpossibilities(p string, known map[string]bool) map[string]bool {
	// a new collection of possibilities has arrived <p1>xxx</p1><p2>yyy</p2>...
	// parse this string for a list of possibilities; then add its elements to the set of known possibilities
	// return the updated set
	pf := regexp.MustCompile(`(<possibility_\d{1,2}>.*?</possibility_\d{1,2}>)`)
	mm := pf.FindAllString(p, -1)
	for i := 0; i < len(mm); i++ {
		known[mm[i]] = true
	}
	return known
}

func getpossiblemorph(o string, p string) MorphPossibility {
	// this still only does a single possibility...

	// see the example at https://golang.org/pkg/regexp/
	// this regex matches the python for good/ill...
	pf := regexp.MustCompile(`(<possibility_(\d{1,2})>)(.*?)<xref_value>(.*?)</xref_value><xref_kind>(.*?)</xref_kind>(.*?)</possibility_\d{1,2}>`)
	m := pf.FindStringSubmatchIndex(p)

	// SAMPLE
	// p := "<possibility_2>bellī, bellus<xref_value>8636495</xref_value><xref_kind>9</xref_kind><transl>A. pretty; B. every thing beautiful; A. Gallant; B. good</transl><analysis>masc nom/voc pl</analysis></possibility_2>"
	// fmt.Printf("%d\n", m)
	// [0 211 0 15 13 14 15 30 42 49 73 74 86 195]
	// [0] 0 211: whole
	// [2] 0 15: <possibility_2>
	// [4] 13 14: 2
	// [6] 15 30: bellī, bellus
	// [8] 42 49: 8636495
	//[10] 73 74: 9
	//[12] 86 195: <transl>A. pretty; B. every thing beautiful; A. Gallant; B. good</transl><analysis>masc nom/voc pl</analysis>

	// should include a test to make sure len(m) will let us do the following? but m is nil if it won't fit the template?
	var mp MorphPossibility
	mp.Observed = o
	x := p[m[6]:m[7]]
	s := strings.Split(x, ",")
	if len(s) == 1 {
		mp.Entry = s[0]
	} else {
		mp.Entry = s[1]
	}
	mp.Entry = strings.TrimSpace(mp.Entry)
	mp.Number = p[m[4]:m[5]]
	mp.TrAnal = p[m[12]:m[13]]
	mp.Xref = p[m[8]:m[9]]
	// fmt.Printf("%s\n", mp)
	return mp
}

func fetchheadwordcounts(headwordset map[string]bool, dbpool *pgxpool.Pool) map[string]int {
	tt := "CREATE TEMPORARY TABLE temporary_headwordlist_%s AS SELECT headwords AS hw FROM unnest(ARRAY[$1]) headwords"
	qt := "SELECT entry_name, total_count FROM dictionary_headword_wordcounts WHERE EXISTS " +
		"(SELECT 1 FROM temporary_headwordlist_%s temptable WHERE temptable.hw = dictionary_headword_wordcounts.entry_name)"
	rndid := strings.Replace(uuid.New().String(), "-", "", -1)

	hw := make([]string, 0, len(headwordset))
	for h := range headwordset {
		hw = append(hw, h)
	}

	arr := strings.Join(hw, ", ")
	_, err := dbpool.Exec(context.Background(), fmt.Sprintf(tt, rndid), arr)
	checkerror(err)

	foundrows, e := dbpool.Query(context.Background(), fmt.Sprintf(qt, rndid))
	checkerror(e)

	returnmap := make(map[string]int)
	defer foundrows.Close()
	for foundrows.Next() {
		var thehit WeightedHeadword
		err = foundrows.Scan(&thehit.Word, &thehit.Count)
		if err != nil {
			fmt.Println(err)
		}
		returnmap[thehit.Word] = thehit.Count
	}
	return returnmap
}

func recursivesplitter(ss []string, tt []string, c int, e int) []string {
	for {
		c += 1
		var rr []string
		var t string

		if len(tt) > 1 {
			t = tt[0]
			tt = tt[1:]
		} else {
			t = tt[0]
		}

		for i := 0; i < len(ss); i++ {
			rr = append(rr, strings.Split(ss[i], t)...)
		}

		//for i := 0; i < len(rr); i++ {
		//	fmt.Printf(fmt.Sprintf("%d.%d: %s\n", c, i, rr[i]))
		//}

		if c < e {
			return recursivesplitter(rr, tt, c, e)
		} else {
			return rr
		}
	}
}

//
// AUTHENTICATION
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
