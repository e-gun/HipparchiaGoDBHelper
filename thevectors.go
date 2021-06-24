//    HipparchiaGoDBHelper: search and vector helper app and functions for HipparchiaServer
//    Copyright: E Gunderson 2021
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
// [i] purge stopwords
// [j] store the bags
//
// once you reach this point python can fetch the bags and then run "Word2Vec(bags, parameters, ...)"
//

package main

import (
	"fmt"
	"github.com/bytedance/sonic"
	"github.com/gomodule/redigo/redis"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

//HipparchiaBagger: Take a key; grab lines; bag them; store them
func HipparchiaBagger(key string, baggingmethod string, goroutines int, thedb string, thestart int, theend int,
	loglevel int, headwordstoskip string, inflectedtoskip string, rl RedisLogin, pl PostgresLogin) string {
	// this does not work at the moment if called as a python module
	// but HipparchiaServer does not know how to call it either...
	smk := key + "_statusmessage"
	logiflogging(fmt.Sprintf("Bagger Module Launched"), loglevel, 1)
	start := time.Now()
	previous := time.Now()
	logiflogging(fmt.Sprintf("Seeking to build *%s* bags of words", baggingmethod), loglevel, 2)

	rc := grabredisconnection(rl)
	defer rc.Close()
	logiflogging(fmt.Sprintf("Connected to redis"), loglevel, 2)

	dbpool := grabpgsqlconnection(pl, goroutines, loglevel)
	defer dbpool.Close()

	// [a] grab the db lines
	// we do this by copying the code inside of grabber but just cut out the storage bits: not DRY, but...

	remain, e := redis.Int64(rc.Do("SCARD", key))
	checkerror(e)
	rcsetint(rc, key+"_poolofwork", remain)

	dblines := make(map[int]DbWorkline)

	if key == "" {
		logiflogging(fmt.Sprintf("No redis key; gathering lines with a direct CLI PostgreSQL query"), loglevel, 1)
		dblines = fetchdblinesdirectly(thedb, thestart, theend, dbpool)
	} else {
		count := 0
		for {
			// [i] get a pre-rolled or break the loop
			thequery, err := redis.String(rc.Do("SPOP", key))
			if err != nil {
				break
			}

			// [ii] - [v] inside findtherows() because its code is common with grabber's needs
			foundrows := findtherows(thequery, "bagger", key, 0, loglevel, rc, dbpool)

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
	timetracker("A", m, start, previous, loglevel)
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
	timetracker("B", m, start, previous, loglevel)
	previous = time.Now()

	// [c] do some preliminary cleanups
	// parsevectorsentences()

	strip := []string{`&nbsp;`, `- `, `<.*?>`}
	thetext = stripper(thetext, strip)

	// this would be a good place to deabbreviate, etc...
	thetext = makesubstitutions(thetext)

	m = fmt.Sprintf("Preliminary cleanups complete")
	rcsetstr(rc, smk, m)
	timetracker("C", m, start, previous, loglevel)
	previous = time.Now()

	// [d] break the text into sentences and assemble []SentenceWithLocus

	ss := splitonpunctuaton(thetext)
	var sentences []SentenceWithLocus
	var first string
	var last string

	const tagger = `⊏(.*?)⊐`
	const notachar = `[^\sa-zα-ωϲϹἀἁἂἃἄἅἆἇᾀᾁᾂᾃᾄᾅᾆᾇᾲᾳᾴᾶᾷᾰᾱὰάἐἑἒἓἔἕὲέἰἱἲἳἴἵἶἷὶίῐῑῒΐῖῗὀὁὂὃὄὅόὸὐὑὒὓὔὕὖὗϋῠῡῢΰῦῧύὺᾐᾑᾒᾓᾔᾕᾖᾗῂῃῄῆῇἤἢἥἣὴήἠἡἦἧὠὡὢὣὤὥὦὧᾠᾡᾢᾣᾤᾥᾦᾧῲῳῴῶῷώὼ]`
	re := regexp.MustCompile(tagger)

	for i := 0; i < len(ss); i++ {
		tags := re.FindAllStringSubmatch(ss[i], -1)
		if len(tags) > 0 {
			first = tags[0][1]
			last = tags[len(tags)-1][1]
		} else {
			first = last
		}
		var sl SentenceWithLocus
		sl.Loc = first
		sl.Sent = strings.ToLower(ss[i])
		sl.Sent = stripper(sl.Sent, []string{tagger, notachar})
		sentences = append(sentences, sl)
	}

	m = fmt.Sprintf("Found %d sentences", len(sentences))
	rcsetstr(rc, smk, m)
	timetracker("D", m, start, previous, loglevel)
	previous = time.Now()

	// unlemmatized bags of words customers have in fact reached their target as of now
	if baggingmethod == "unlemmatized" {
		sentences = dropstopwords(inflectedtoskip, sentences)
		kk := strings.Split(key, "_")
		resultkey := kk[0] + "_vectorresults"
		loadthebags(resultkey, goroutines, sentences, rl)
		// DO NO comment out the fmt.Printf(): the resultkey is parsed by HipparchiaServer
		// "resultrediskey = resultrediskey.split()[-1]"
		fmt.Println(fmt.Sprintf("%d %s bags of words stored at %s", len(sentences), baggingmethod, resultkey))
		os.Exit(0)
	}

	// ex sentence: {line/lt0448w001/22  Belgae ab extremis Galliae finibus oriuntur, pertinent ad inferiorem partem fluminis Rheni, spectant in septentrionem et orientem solem}

	// [e] figure out all of the words used in the passage

	// generate a "set" via make(map[string]bool)
	allwords := make(map[string]bool, len(sentences))
	for i := 0; i < len(sentences); i++ {
		ww := strings.Split(sentences[i].Sent, " ")
		for j := 0; j < len(ww); j++ {
			allwords[ww[j]] = true
		}
	}

	m = fmt.Sprintf("Found %d distinct words", len(allwords))
	rcsetstr(rc, smk, m)
	timetracker("E", m, start, previous, loglevel)
	previous = time.Now()

	// [f] find all of the parsing info relative to these words

	// can only send the keys to getrequiredmorphobjects(); so we need to demap things
	thewords := make([]string, 0, len(allwords))
	for w := range allwords {
		thewords = append(thewords, w)
	}

	var mo map[string]DbMorphology
	mo = getrequiredmorphobjects(thewords, goroutines, pl)

	m = fmt.Sprintf("Got morphology for %d terms", len(mo))
	rcsetstr(rc, smk, m)
	timetracker("F", m, start, previous, loglevel)
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
	timetracker("G", m, start, previous, loglevel)
	previous = time.Now()

	// [h] build the lemmatized bags of words

	switch baggingmethod {
	// see vectorparsingandbagging.go
	case "flat":
		sentences = buildflatbagsofwords(sentences, morphdict)
	case "alternates":
		sentences = buildcompositebagsofwords(sentences, morphdict)
	case "winnertakesall":
		sentences = buildwinnertakesallbagsofwords(sentences, morphdict, dbpool)
	default:
		m = fmt.Sprintf("unknown bagging method '%s'; storing unlemmatized bags", baggingmethod)
		logiflogging(m, loglevel, 0)
	}

	m = fmt.Sprintf("Finished bagging %d bags", len(sentences))
	rcsetstr(rc, smk, m)
	timetracker("H", m, start, previous, loglevel)
	previous = time.Now()

	// [i] purge stopwords
	sentences = dropstopwords(headwordstoskip, sentences)
	sentences = dropstopwords(inflectedtoskip, sentences)

	var clearedlist []SentenceWithLocus
	for i := 0; i < len(sentences); i++ {
		if sentences[i].Sent != "" {
			clearedlist = append(clearedlist, sentences[i])
		}
	}

	sentences = clearedlist

	m = fmt.Sprintf("Cleared stopwords: %d bags remain", len(sentences))
	rcsetstr(rc, smk, m)
	timetracker("I", m, start, previous, loglevel)
	previous = time.Now()

	// [J] store...
	kk := strings.Split(key, "_")
	resultkey := kk[0] + "_vectorresults"

	loadthebags(resultkey, goroutines, sentences, rl)

	m = fmt.Sprintf("Finished loading")
	rcsetstr(rc, smk, m)
	timetracker("J", m, start, previous, loglevel)
	previous = time.Now()

	rcsetint(rc, key+"_poolofwork", -1)
	rcsetint(rc, key+"_hitcount", 0)

	return resultkey
}

func loadthebags(resultkey string, goroutines int, sentences []SentenceWithLocus, rl RedisLogin) {
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
		go parallelredisloader(i, resultkey, bagsofbags[i], rl, &wg)
	}
	wg.Wait()
}

func parallelredisloader(workerid int, resultkey string, bags []SentenceWithLocus, rl RedisLogin, wg *sync.WaitGroup) {
	// make sure that "0" comes in last so you can watch the parallelism
	//if workerid == 0 {
	//	time.Sleep(pollinginterval)
	//	time.Sleep(pollinginterval)
	//}

	rc := grabredisconnection(rl)

	for i := 0; i < len(bags); i++ {
		jsonhit, err := sonic.Marshal(bags[i])
		checkerror(err)
		rcsadd(rc, resultkey, jsonhit)
	}

	wg.Done()
}

func timetracker(letter string, m string, start time.Time, previous time.Time, loglevel int) {
	d := fmt.Sprintf("[Δ: %.3fs] ", time.Now().Sub(previous).Seconds())
	m = fmt.Sprintf("[%s: %.3fs]", letter, time.Now().Sub(start).Seconds()) + d + m
	logiflogging(m, loglevel, 3)
}
