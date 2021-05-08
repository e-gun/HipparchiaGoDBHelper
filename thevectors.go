//    HipparchiaGoDBHelper: search and vector helper app and functions for HipparchiaServer
//    Copyright: E Gunderson 2016-21
//    License: GNU GENERAL PUBLIC LICENSE 3
//        (see LICENSE in the top level directory of the distribution)

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

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

//HipparchiaBagger: Take a key; grab lines; bag them; store them
func HipparchiaBagger(searchkey string, baggingmethod string, goroutines int, thedb string, thestart int, theend int,
	loglevel int, r RedisLogin, p PostgresLogin) string {

	logiflogging(fmt.Sprintf("Bagger Module Launched"), loglevel, 1)
	start := time.Now()
	logiflogging(fmt.Sprintf("Seeking to build *%s* bags of words", baggingmethod), loglevel, 2)

	redisclient := redis.NewClient(&redis.Options{Addr: r.Addr, Password: r.Password, DB: r.DB})
	_, err := redisclient.Ping().Result()
	checkerror(err)
	defer redisclient.Close()
	logiflogging(fmt.Sprintf("Connected to redis"), loglevel, 2)

	// turn of progress logging
	redisclient.Set(searchkey+"_poolofwork", -1, redisexpiration)
	redisclient.Set(searchkey+"_hitcount", 0, redisexpiration)

	url := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", p.User, p.Pass, p.Host, p.Port, p.DBName)
	dbpool, err := pgxpool.Connect(context.Background(), url)
	if err != nil {
		logiflogging(fmt.Sprintf("Could not connect to PostgreSQL via %s", url), loglevel, 0)
		panic(err)
	}

	defer dbpool.Close()
	logiflogging(fmt.Sprintf("Connected to %s on PostgreSQL", p.DBName), loglevel, 2)

	// [a] grab the db lines
	// we do this by copying the code inside of grabber but just cut out the storage bits: not DRY, but...
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
			// [i] get a query
			byteArray, err := redisclient.SPop(searchkey).Result()
			if err != nil {
				break
			}

			remain, err := redisclient.SCard(searchkey).Result()
			checkerror(err)
			logiflogging(fmt.Sprintf("bagger says that %d locations still need grabbing", remain), loglevel, 3)

			// [ii] decode the query
			var prq PrerolledQuery
			err = json.Unmarshal([]byte(byteArray), &prq)
			checkerror(err)

			// [iii] build a temp table if needed
			if prq.TempTable != "" {
				_, err := dbpool.Exec(context.Background(), prq.TempTable)
				checkerror(err)
			}

			// [iv] execute the main query
			var foundrows pgx.Rows
			logiflogging(fmt.Sprintf("bagger will ask: %s", prq.PsqlQuery), loglevel, 5)
			if prq.PsqlData != "" {
				foundrows, err = dbpool.Query(context.Background(), prq.PsqlQuery, prq.PsqlData)
				checkerror(err)
			} else {
				foundrows, err = dbpool.Query(context.Background(), prq.PsqlQuery)
				checkerror(err)
			}

			// [v] iterate through the finds
			defer foundrows.Close()
			for foundrows.Next() {
				count += 1
				// convert the find to a DbWorkline
				var thehit DbWorkline
				err = foundrows.Scan(&thehit.WkUID, &thehit.TbIndex, &thehit.Lvl5Value, &thehit.Lvl4Value, &thehit.Lvl3Value,
					&thehit.Lvl2Value, &thehit.Lvl1Value, &thehit.Lvl0Value, &thehit.MarkedUp, &thehit.Accented,
					&thehit.Stripped, &thehit.Hypenated, &thehit.Annotations)
				checkerror(err)
				dblines[count] = thehit
			}
		}
	}

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

	// unlemmatized bags of words customers have in fact reached their target as of now
	if baggingmethod == "unlemmatized" {
		kk := strings.Split(searchkey, "_")
		resultkey := kk[0] + "_vectorresults"
		loadthebags(resultkey, goroutines, sentences, redisclient)
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

	// the above is interestingly slow... not super-slow, but still relatively slow: 3s in Cicero
	logiflogging(fmt.Sprintf("Build morphdict [F1: %fs]", time.Now().Sub(start).Seconds()), loglevel, 5)

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
	logiflogging(fmt.Sprintf("Pre-Bagging [F2: %fs]", time.Now().Sub(start).Seconds()), loglevel, 5)

	switch baggingmethod {
	case "flat":
		sentences = buildflatbagsofwords(sentences, flatdict)
	case "alternates":
		sentences = buildcompositebagsofwords(sentences, flatdict)
	case "winnertakesall":
		sentences = buildwinnertakesallbagsofwords(sentences, flatdict, dbpool)
	default:
		logiflogging(fmt.Sprintf("unknown bagging method '%s'; storing unlemmatized bags", baggingmethod), loglevel, 0)
	}
	logiflogging(fmt.Sprintf("Post-Bagging [F3: %fs]", time.Now().Sub(start).Seconds()), loglevel, 5)

	kk := strings.Split(searchkey, "_")
	resultkey := kk[0] + "_vectorresults"

	loadthebags(resultkey, goroutines, sentences, redisclient)

	logiflogging(fmt.Sprintf("Reached result @ %fs]", time.Now().Sub(start).Seconds()), loglevel, 1)
	return resultkey
}

func loadthebags(resultkey string, goroutines int, sentences []SentenceWithLocus, redisclient *redis.Client) {
	totalwork := len(sentences)
	chunksize := totalwork / goroutines
	leftover := totalwork % goroutines
	bagsofbags := make(map[int][]SentenceWithLocus, goroutines)

	if totalwork <= goroutines {
		bagsofbags[0] = sentences
	} else {
		thestart := 0
		for i := 0; i < goroutines; i++ {
			bagsofbags[i] = sentences[thestart : thestart+chunksize]
			thestart = thestart + chunksize
		}

		// leave no sentence behind!
		if leftover > 0 {
			bagsofbags[goroutines-1] = append(bagsofbags[goroutines-1], sentences[totalwork-leftover-1:totalwork-1]...)
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go parallelredisloader(resultkey, bagsofbags[i], redisclient, &wg)
	}
	wg.Wait()
}
