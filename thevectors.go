//    HipparchiaGoDBHelper: search and vector helper app and functions for HipparchiaServer
//    Copyright: E Gunderson 2021
//    License: GNU GENERAL PUBLIC LICENSE 3
//        (see LICENSE in the top level directory of the distribution)

// VECTOR PREP builds bags for modeling; to do this you need to...

// [a] grab db lines that are relevant to the search
// [b] turn them into a unified text block
// [c] do some preliminary cleanups
// [d] break the text into sentences and assemble []BagWithLocus (NB: these are "unlemmatized bags of words")
// [e] figure out all of the words used in the passage
// [f] find all of the parsing info relative to these words
// [g] figure out which headwords to associate with the collection of words
// [h] build the lemmatized bags of words ('unlemmatized' can skip [f] and [g]...)
// [i] purge stopwords
// [j] store the bags
//
// once you reach this point python can fetch the bags and then run "Word2Vec(bags, parameters, ...)"
//

package hipparchiagolangsearching

import (
	"encoding/json"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

// ExternalBagger : Take a key; grab lines; bag them; store them. This is a hook for calling via the
// module interface for HipparchiaServer. Module use currently not implemented on the server side.
// Instead the CLI will send you to HipparchiaVectors()
func ExternalBagger(key string, baggingmethod string, goroutines int, bagsize int, thedb string, thestart int, theend int,
	loglevel int, headwordstoskip string, inflectedtoskip string, rl RedisLogin, pl PostgresLogin) string {

	cfg.RedisKey = key
	cfg.BagMethod = baggingmethod
	cfg.WorkerCount = goroutines
	cfg.SentPerBag = bagsize
	cfg.VectTestDB = thedb
	cfg.VectStart = thestart
	cfg.VectEnd = theend
	cfg.LogLevel = loglevel
	cfg.VSkipHW = headwordstoskip
	cfg.VSkipInf = inflectedtoskip
	cfg.RLogin = rl
	cfg.PGLogin = pl

	results := HipparchiaVectors()

	return results
}

func HipparchiaVectors() string {
	// this does not work at the moment if called as a python module
	// but HipparchiaServer does not know how to call it either...
	key := cfg.RedisKey
	smk := key + "_statusmessage"
	msg(fmt.Sprintf("Vector Bagger Launched"), 1)
	start := time.Now()
	previous := time.Now()
	msg(fmt.Sprintf("Seeking to build *%s* bags of words", cfg.BagMethod), 2)

	rc := grabredisconnection()
	defer func(rc redis.Conn) {
		err := rc.Close()
		checkerror(err)
	}(rc)

	msg(fmt.Sprintf("Connected to redis"), 2)

	dbpool := grabpgsqlconnection()
	defer dbpool.Close()

	// [a] grab the db lines
	// we do this by copying the code inside of grabber but just cut out the storage bits: not DRY, but...

	remain, e := redis.Int64(rc.Do("SCARD", key))
	checkerror(e)
	rcsetint(rc, key+"_poolofwork", remain)

	dblines := make(map[int]DbWorkline)

	if key == "" {
		msg(fmt.Sprintf("No redis key; gathering lines with a direct CLI PostgreSQL query"), 1)
		dblines = fetchdblinesdirectly(dbpool)
	} else {
		count := 0
		for {
			// [i] get a pre-rolled or break the loop
			thequery, err := redis.String(rc.Do("SPOP", key))
			if err != nil {
				break
			}

			// [ii] - [v] inside findtherows() because its code is common with grabber's needs
			foundrows := findtherows(thequery, "bagger", key, 0, rc, dbpool)

			// [vi] iterate through the finds
			defer foundrows.Close()
			for foundrows.Next() {
				count += 1
				if count%1000 == 0 {
					rcsetint(rc, key+"_hitcount", int64(count))
				}
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

	m := fmt.Sprintf("%d lines acquired", len(dblines))
	rcsetstr(rc, smk, m)
	timetracker("A", m, start, previous)
	previous = time.Now()

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

	thetext := sb.String()
	sb.Reset()

	m = fmt.Sprintf("Unified text block built")
	rcsetstr(rc, smk, m)
	timetracker("B", m, start, previous)
	previous = time.Now()

	// [c] do some preliminary cleanups
	// parsevectorsentences()

	strip := []string{`&nbsp;`, `- `, `<.*?>`}
	thetext = stripper(thetext, strip)

	// this would be a good place to deabbreviate, etc...
	thetext = makesubstitutions(thetext)
	thetext = acuteforgrave(thetext)

	m = fmt.Sprintf("Preliminary cleanups complete")
	rcsetstr(rc, smk, m)
	timetracker("C", m, start, previous)
	previous = time.Now()

	// [d] break the text into sentences and assemble []BagWithLocus

	split := splitonpunctuaton(thetext)

	// empty sentences via "..."? not much of an issue: Cicero goes from 68790 to 68697
	// this will cost you c. .03s

	var ss []string
	for i := 0; i < len(split); i++ {
		if len(split[i]) > 0 {
			ss = append(ss, split[i])
		}
	}

	var thebags []BagWithLocus
	var first string
	var last string

	const tagger = `⊏(.*?)⊐`
	const notachar = `[^\sa-zα-ωϲῥἀἁἂἃἄἅἆἇᾀᾁᾂᾃᾄᾅᾆᾇᾲᾳᾴᾶᾷᾰᾱὰάἐἑἒἓἔἕὲέἰἱἲἳἴἵἶἷὶίῐῑῒΐῖῗὀὁὂὃὄὅόὸὐὑὒὓὔὕὖὗϋῠῡῢΰῦῧύὺᾐᾑᾒᾓᾔᾕᾖᾗῂῃῄῆῇἤἢἥἣὴήἠἡἦἧὠὡὢὣὤὥὦὧᾠᾡᾢᾣᾤᾥᾦᾧῲῳῴῶῷώὼ]`
	re := regexp.MustCompile(tagger)

	totalsent := len(ss)
	iterations := len(ss) / cfg.SentPerBag
	index := 0
	for i := 0; i < iterations; i++ {
		parcel := strings.Join(ss[index:index+cfg.SentPerBag], " ")
		index = index + cfg.SentPerBag
		tags := re.FindAllStringSubmatch(parcel, -1)
		if len(tags) > 0 {
			first = tags[0][1]
			last = tags[len(tags)-1][1]
		} else {
			first = last
		}
		var sl BagWithLocus
		sl.Loc = first
		sl.Bag = strings.ToLower(parcel)
		sl.Bag = stripper(sl.Bag, []string{tagger, notachar})
		thebags = append(thebags, sl)
	}

	//for i := 0; i < len(sentences); i++ {
	//	fmt.Println(fmt.Sprintf("[%d] %s: %s", i, sentences[i].Loc, sentences[i].Bag))
	//}

	m = fmt.Sprintf("Inserted %d sentences into %d bags", totalsent, len(thebags))
	rcsetstr(rc, smk, m)
	timetracker("D", m, start, previous)
	previous = time.Now()

	// unlemmatized bags of words customers have in fact reached their target as of now
	if cfg.BagMethod == "unlemmatized" {
		thebags = dropstopwords(cfg.VSkipInf, thebags)
		kk := strings.Split(key, "_")
		resultkey := kk[0] + "_vectorresults"
		loadthebags(resultkey, thebags)
		// DO NOT comment out the fmt.Printf(): the resultkey is parsed by HipparchiaServer
		// "resultrediskey = resultrediskey.split()[-1]"
		fmt.Printf("%d %s bags of words stored at %s", len(thebags), cfg.BagMethod, resultkey)
		os.Exit(0)
	}

	// ex sentence: {line/lt0448w001/22  Belgae ab extremis Galliae finibus oriuntur, pertinent ad inferiorem partem fluminis Rheni, spectant in septentrionem et orientem solem}

	// [e] figure out all of the words used in the passage

	// generate a "set" via make(map[string]bool)
	allwords := make(map[string]bool, len(thebags))
	for i := 0; i < len(thebags); i++ {
		ww := strings.Split(thebags[i].Bag, " ")
		for j := 0; j < len(ww); j++ {
			allwords[ww[j]] = true
		}
	}

	m = fmt.Sprintf("Found %d distinct words", len(allwords))
	rcsetstr(rc, smk, m)
	timetracker("E", m, start, previous)
	previous = time.Now()

	// [f] find all of the parsing info relative to these words

	// can only send the keys to getrequiredmorphobjects(); so we need to demap things
	thewords := make([]string, 0, len(allwords))
	for w := range allwords {
		thewords = append(thewords, w)
	}

	var mo map[string]DbMorphology
	mo = getrequiredmorphobjects(thewords)

	m = fmt.Sprintf("Got morphology for %d terms", len(mo))
	rcsetstr(rc, smk, m)
	timetracker("F", m, start, previous)
	previous = time.Now()

	// [g] figure out which headwords to associate with the collection of words
	// this information now already inside of DbMorphology.RawPossib which grabs "related_headwords" from the DB table
	// a set of sets, as it were:
	//		key = word-in-use
	//		value = { maybeA, maybeB, maybeC}
	// {'θεῶν': {'θεόϲ', 'θέα', 'θεάω', 'θεά'}, 'πώ': {'πω'}, 'πολλά': {'πολύϲ'}, 'πατήρ': {'πατήρ'}, ... }

	morphdict := make(map[string][]string, len(mo))
	for m := range mo {
		morphdict[m] = strings.Split(mo[m].RawPossib, " ")
	}

	// [HGH] [E: 1.453s][Δ: 0.061s] Found 80125 distinct words
	// [HGH] [F: 1.788s][Δ: 0.335s] Got morphology for 73819 terms
	// if you just iterate over mo, you drop unparsed terms: retain them

	for w := range allwords {
		if _, t := morphdict[w]; t {
			continue
		} else {
			morphdict[w] = []string{w}
		}
	}

	m = fmt.Sprintf("Built morphmap for %d terms", len(morphdict))
	rcsetstr(rc, smk, m)
	timetracker("G", m, start, previous)
	previous = time.Now()

	// [h] build the lemmatized bags of words

	switch cfg.BagMethod {
	// see vectorparsingandbagging.go
	case "flat":
		thebags = buildflatbagsofwords(thebags, morphdict)
	case "alternates":
		thebags = buildcompositebagsofwords(thebags, morphdict)
	case "winnertakesall":
		thebags = buildwinnertakesallbagsofwords(thebags, morphdict, dbpool)
	default:
		m = fmt.Sprintf("unknown bagging method '%s'; storing unlemmatized bags", cfg.BagMethod)
		msg(m, 0)
	}

	m = fmt.Sprintf("Finished bagging %d bags", len(thebags))
	rcsetstr(rc, smk, m)
	timetracker("H", m, start, previous)
	previous = time.Now()

	// [i] purge stopwords
	thebags = dropstopwords(cfg.VSkipHW, thebags)
	thebags = dropstopwords(cfg.VSkipInf, thebags)

	var clearedlist []BagWithLocus
	for i := 0; i < len(thebags); i++ {
		if thebags[i].Bag != "" {
			clearedlist = append(clearedlist, thebags[i])
		}
	}

	thebags = clearedlist

	m = fmt.Sprintf("Cleared stopwords: %d bags remain", len(thebags))
	rcsetstr(rc, smk, m)
	timetracker("I", m, start, previous)
	previous = time.Now()

	// [J] store...

	kk := strings.Split(key, "_")
	resultkey := kk[0] + "_vectorresults"

	loadthebags(resultkey, thebags)

	m = fmt.Sprintf("Finished loading")
	rcsetstr(rc, smk, m)
	timetracker("J", m, start, previous)
	previous = time.Now()

	rcsetint(rc, key+"_poolofwork", -1)
	rcsetint(rc, key+"_hitcount", 0)

	return resultkey
}

func loadthebags(resultkey string, sentences []BagWithLocus) {
	totalwork := len(sentences)
	chunksize := totalwork / cfg.WorkerCount
	leftover := totalwork % cfg.WorkerCount
	bagsofbags := make(map[int][]BagWithLocus, cfg.WorkerCount)

	if totalwork <= cfg.WorkerCount {
		bagsofbags[0] = sentences
	} else {
		thestart := 0
		for i := 0; i < cfg.WorkerCount; i++ {
			bagsofbags[i] = sentences[thestart : thestart+chunksize]
			thestart = thestart + chunksize
		}

		// leave no sentence behind!
		if leftover > 0 {
			bagsofbags[cfg.WorkerCount-1] = append(bagsofbags[cfg.WorkerCount-1], sentences[totalwork-leftover-1:totalwork-1]...)
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < cfg.WorkerCount; i++ {
		wg.Add(1)
		go parallelredisloader(i, resultkey, bagsofbags[i], &wg)
	}
	wg.Wait()
}

func parallelredisloader(workerid int, resultkey string, bags []BagWithLocus, wg *sync.WaitGroup) {
	// make sure that "0" comes in last so you can watch the parallelism
	//if workerid == 0 {
	//	time.Sleep(pollinginterval)
	//	time.Sleep(pollinginterval)
	//}

	rc := grabredisconnection()
	defer func(rc redis.Conn) {
		err := rc.Close()
		checkerror(err)
	}(rc)

	// pipleline it...
	// https://pkg.go.dev/github.com/gomodule/redigo/redis#hdr-Pipelining
	// 	Connections support pipelining using the Send, Flush and Receive methods.

	err := rc.Send("MULTI")
	checkerror(err)

	for i := 0; i < len(bags); i++ {
		jsonhit, ee := json.Marshal(bags[i])
		checkerror(ee)

		e := rc.Send("SADD", resultkey, jsonhit)
		checkerror(e)
	}

	_, e := rc.Do("EXEC")
	checkerror(e)

	wg.Done()
}

func timetracker(letter string, m string, start time.Time, previous time.Time) {
	d := fmt.Sprintf("[Δ: %.3fs] ", time.Now().Sub(previous).Seconds())
	m = fmt.Sprintf("[%s: %.3fs]", letter, time.Now().Sub(start).Seconds()) + d + m
	msg(m, 3)
}
