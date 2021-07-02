//    HipparchiaGoDBHelper: search, vector and websocket helper app and functions for HipparchiaServer
//    Copyright: E Gunderson 2021
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
// [d] break the text into sentences and assemble []BagWithLocus (NB: these are "unlemmatized bags of words")
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

// Usage of ./HipparchiaGoDBHelper:
//  -c int
//    	[searches] max hit count (default 200)
//  -k string
//    	[searches] redis key to use
//  -l int
//    	[common] logging level: 0 is silent; 5 is very noisy (default 1)
//  -p string
//    	[common] psql logon information (as a JSON string) (default "{\"Host\": \"localhost\", \"Port\": 5432, \"User\": \"hippa_wr\", \"Pass\": \"\", \"DBName\": \"hipparchiaDB\"}")
//  -profile
//    	[debugging] profile cpu use to './profiler_output.bin'
//  -r string
//    	[common] redis logon information (as a JSON string) (default "{\"Addr\": \"localhost:6379\", \"Password\": \"\", \"DB\": 0}")
//  -sv
//    	[vectors] assert that this is a vectorizing run
//  -svb string
//    	[vectors] the bagging method: choices are alternates, flat, unlemmatized, winnertakesall (default "winnertakesall")
//  -svbs int
//    	[vectors] number of sentences per bag (default 1)
//  -svdb string
//    	[vectors][for manual debugging] db to grab from (default "lt0448")
//  -sve int
//    	[vectors][for manual debugging] last line to grab (default 26)
//  -svhw string
//    	[vectors] provide a string of headwords to skip 'one two three...' (default "(suppressed owing to length)")
//  -svin string
//    	[vectors][provide a string of inflected forms to skip 'one two three...' (default "(suppressed owing to length)")
//  -svs int
//    	[vectors][for manual debugging] first line to grab (default 1)
//  -t int
//    	[common] number of goroutines to dispatch (default 5)
//  -v	[common] print version and exit
//  -ws
//    	[websockets] assert that you are requesting the websocket server
//  -wsf int
//    	[websockets] fail threshold before messages stop being sent (default 3)
//  -wsp int
//    	[websockets] port on which to open the websocket server (default 5010)
//  -wss int
//    	[websockets] save the polls instead of deleting them: 0 is no; 1 is yes

// toggle the package name to shift between cli and module builds: main or hipparchiagolangsearching
package main

import (
	"C"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime/pprof"
	"time"
)

const (
	myname          = "Hipparchia Golang Helper"
	shortname       = "HGH"
	version         = "1.3.3"
	tesquery        = "SELECT * FROM %s WHERE index BETWEEN %d and %d"
	testdb          = "lt0448"
	teststart       = 1
	testend         = 26
	linelength      = 72
	pollinginterval = 333 * time.Millisecond
	skipheadwords   = "unus verum omne sum¹ ab δύο πρότεροϲ ἄνθρωποϲ τίϲ δέω¹ ὅϲτιϲ homo πᾶϲ οὖν εἶπον ἠμί ἄν² tantus μένω μέγαϲ οὐ verus neque eo¹ nam μέν ἡμόϲ aut Sue διό reor ut ἐγώ is πωϲ ἐκάϲ enim ὅτι² παρά ἐν Ἔχιϲ sed ἐμόϲ οὐδόϲ ad de ita πηρόϲ οὗτοϲ an ἐπεί a γάρ αὐτοῦ ἐκεῖνοϲ ἀνά ἑαυτοῦ quam αὐτόϲε et ὑπό quidem Alius¹ οἷοϲ noster γίγνομαι ἄνα προϲάμβ ἄν¹ οὕτωϲ pro² tamen ἐάν atque τε qui² si multus idem οὐδέ ἐκ omnes γε δεῖ πολύϲ in ἔδω ὅτι¹ μή Ios ἕτεροϲ cum meus ὅλοξ suus omnis ὡϲ sua μετά Ἀλλά ne¹ jam εἰϲ ἤ² ἄναξ ἕ ὅϲοϲ dies ipse ὁ hic οὐδείϲ suo ἔτι ἄνω¹ ὅϲ νῦν ὁμοῖοϲ edo¹ εἰ qui¹ πάλιν ὥϲπερ ne³ ἵνα τιϲ διά φύω per τοιοῦτοϲ for eo² huc locum neo¹ sui non ἤ¹ χάω ex κατά δή ἁμόϲ ὅμοιοϲ αὐτόϲ etiam vaco πρόϲ Ζεύϲ ϲύ quis¹ tuus b εἷϲ Eos οὔτε τῇ καθά ego tu ille pro¹ ἀπό suum εἰμί ἄλλοϲ δέ alius² pars vel ὥϲτε χέω res ἡμέρα quo δέομαι modus ὑπέρ ϲόϲ ito τῷ περί Τήιοϲ ἕκαϲτοϲ autem καί ἐπί nos θεάω γάρον γάροϲ Cos²"
	skipinflected   = "ita a inquit ego die nunc nos quid πάντων ἤ με θεόν δεῖ for igitur ϲύν b uers p ϲου τῷ εἰϲ ergo ἐπ ὥϲτε sua me πρό sic aut nisi rem πάλιν ἡμῶν φηϲί παρά ἔϲτι αὐτῆϲ τότε eos αὐτούϲ λέγει cum τόν quidem ἐϲτιν posse αὐτόϲ post αὐτῶν libro m hanc οὐδέ fr πρῶτον μέν res ἐϲτι αὐτῷ οὐχ non ἐϲτί modo αὐτοῦ sine ad uero fuit τοῦ ἀπό ea ὅτι parte ἔχει οὔτε ὅταν αὐτήν esse sub τοῦτο i omnes break μή ἤδη ϲοι sibi at mihi τήν in de τούτου ab omnia ὃ ἦν γάρ οὐδέν quam per α autem eius item ὡϲ sint length οὗ λόγον eum ἀντί ex uel ἐπειδή re ei quo ἐξ δραχμαί αὐτό ἄρα ἔτουϲ ἀλλ οὐκ τά ὑπέρ τάϲ μάλιϲτα etiam haec nihil οὕτω siue nobis si itaque uac erat uestig εἶπεν ἔϲτιν tantum tam nec unde qua hoc quis iii ὥϲπερ semper εἶναι e ½ is quem τῆϲ ἐγώ καθ his θεοῦ tibi ubi pro ἄν πολλά τῇ πρόϲ l ἔϲται οὕτωϲ τό ἐφ ἡμῖν οἷϲ inter idem illa n se εἰ μόνον ac ἵνα ipse erit μετά μοι δι γε enim ille an sunt esset γίνεται omnibus ne ἐπί τούτοιϲ ὁμοίωϲ παρ causa neque cr ἐάν quos ταῦτα h ante ἐϲτίν ἣν αὐτόν eo ὧν ἐπεί οἷον sed ἀλλά ii ἡ t te ταῖϲ est sit cuius καί quasi ἀεί o τούτων ἐϲ quae τούϲ minus quia tamen iam d διά primum r τιϲ νῦν illud u apud c ἐκ δ quod f quoque tr τί ipsa rei hic οἱ illi et πῶϲ φηϲίν τοίνυν s magis unknown οὖν dum text μᾶλλον λόγοϲ habet τοῖϲ qui αὐτοῖϲ suo πάντα uacat τίϲ pace ἔχειν οὐ κατά contra δύο ἔτι αἱ uet οὗτοϲ deinde id ut ὑπό τι lin ἄλλων τε tu ὁ cf δή potest ἐν eam tum μου nam θεόϲ κατ ὦ cui nomine περί atque δέ quibus ἡμᾶϲ τῶν eorum"
)

//
// see THEGRABBER.GO, THEVECTORS.GO, and THEWEBSOCKETS.GO for the basic branches of HipparchiaGoDBHelper
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

	flag.StringVar(&k, "k", "", "[searches] redis key to use")
	flag.Int64Var(&c, "c", 200, "[searches] max hit count")
	flag.IntVar(&g, "t", 5, "[common] number of goroutines to dispatch")
	flag.IntVar(&l, "l", 1, "[common] logging level: 0 is silent; 5 is very noisy")
	flag.StringVar(&r, "r", RP, "[common] redis logon information (as a JSON string)")
	flag.StringVar(&p, "p", PSQ, "[common] psql logon information (as a JSON string)")

	// vector flags

	var dbdb string
	var dbdbs int
	var dbdbe int
	var bs int
	var b string
	var hw string
	var in string

	flag.StringVar(&b, "svb", "winnertakesall", "[vectors] the bagging method: choices are alternates, flat, unlemmatized, winnertakesall")
	flag.IntVar(&bs, "svbs", 1, "[vectors] number of sentences per bag")
	flag.StringVar(&dbdb, "svdb", testdb, "[vectors][for manual debugging] db to grab from")
	flag.IntVar(&dbdbs, "svs", teststart, "[vectors][for manual debugging] first line to grab")
	flag.IntVar(&dbdbe, "sve", testend, "[vectors][for manual debugging] last line to grab")
	flag.StringVar(&hw, "svhw", "(suppressed owing to length)", "[vectors] provide a string of headwords to skip 'one two three...'")
	flag.StringVar(&in, "svin", "(suppressed owing to length)", "[vectors][provide a string of inflected forms to skip 'one two three...'")
	sv := flag.Bool("sv", false, "[vectors] assert that this is a vectorizing run")

	if hw == "(suppressed owing to length)" {
		hw = skipheadwords
	}

	if in == "(suppressed owing to length)" {
		in = skipinflected
	}

	// websocket flags

	var wsp int
	var wsf int
	var wss int

	ws := flag.Bool("ws", false, "[websockets] assert that you are requesting the websocket server")
	flag.IntVar(&wsp, "wsp", 5010, "[websockets] port on which to open the websocket server")
	flag.IntVar(&wsf, "wsf", 3, "[websockets] fail threshold before messages stop being sent")
	flag.IntVar(&wss, "wss", 0, "[websockets] save the polls instead of deleting them: 0 is no; 1 is yes")

	v := flag.Bool("v", false, "[common] print version and exit")
	profile := flag.Bool("profile", false, "[debugging] profile cpu use to './profiler_output.bin'")

	flag.Parse()

	if *profile {
		o := "profiler_output.bin"
		f, err := os.Create(o)
		if err != nil {
			logiflogging(fmt.Sprintf("failed to create '%s'", o), 0, 0)
			checkerror(err)
		} else {
			logiflogging(fmt.Sprintf("logging profiling data to '%s'", o), 0, 0)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

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
		o = HipparchiaBagger(k, b, g, bs, dbdb, dbdbs, dbdbe, l, hw, in, rl, po)
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

	// DO NOT comment out the fmt.Printf(): the resultkey is parsed by HipparchiaServer when EXTERNALLOADING = 'cli'
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
