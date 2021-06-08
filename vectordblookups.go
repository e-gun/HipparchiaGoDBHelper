//    HipparchiaGoDBHelper: search and vector helper app and functions for HipparchiaServer
//    Copyright: E Gunderson 2016-21
//    License: GNU GENERAL PUBLIC LICENSE 3
//        (see LICENSE in the top level directory of the distribution)

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4/pgxpool"
	"regexp"
	"strings"
	"sync"
)

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

func getrequiredmorphobjects(wordlist []string, workers int, dbpool *pgxpool.Pool) map[string]DbMorphology {
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

	lt := arraytogetrequiredmorphobjects(latinwords, "latin", workers, dbpool)
	gk := arraytogetrequiredmorphobjects(greekwords, "greek", workers, dbpool)

	mo := make(map[string]DbMorphology)
	for k, v := range gk {
		mo[k] = v
	}

	for k, v := range lt {
		mo[k] = v
	}

	return mo
}

func arraytogetrequiredmorphobjects(wordlist []string, uselang string, workercount int, dbpool *pgxpool.Pool) map[string]DbMorphology {
	// NB: this goroutine version not in fact faster with Cicero than doing it without goroutines as one giant array
	// but the implementation pattern is likely useful for some place where it will make a difference

	// look for the upper case matches too: Ϲωκράτηϲ and not just ϲωκρατέω (!)
	var uppers []string
	for i := 0; i < len(wordlist); i++ {
		uppers = append(uppers, strings.Title(wordlist[i]))
	}

	wordlist = append(wordlist, uppers...)

	totalwork := len(wordlist)
	chunksize := totalwork / workercount
	leftover := totalwork % workercount
	wordmap := make(map[int][]string, workercount)

	if totalwork <= workercount {
		wordmap[0] = wordlist
	} else {
		thestart := 0
		for i := 0; i < workercount; i++ {
			wordmap[i] = wordlist[thestart : thestart+chunksize]
			thestart = thestart + chunksize
		}

		// leave no sentence behind!
		if leftover > 0 {
			wordmap[workercount-1] = append(wordmap[workercount-1], wordlist[totalwork-leftover-1:totalwork-1]...)
		}
	}

	// https://stackoverflow.com/questions/46010836/using-goroutines-to-process-values-and-gather-results-into-a-slice
	// see the comments of Paul Hankin

	var wg sync.WaitGroup
	var collector []map[string]DbMorphology
	outputchannels := make(chan map[string]DbMorphology, workercount)

	for i := 0; i < workercount; i++ {
		wg.Add(1)
		// i will be captured if sent into the function
		j := i
		go func(wordlist []string, uselang string, workerid int, dbpool *pgxpool.Pool) {
			defer wg.Done()
			outputchannels <- arraytmorphologyworker(wordmap[j], uselang, j, 0, dbpool)
		}(wordmap[i], uselang, i, dbpool)
	}

	go func() {
		wg.Wait()
		close(outputchannels)
	}()

	// merge the results
	for c := range outputchannels {
		collector = append(collector, c)
	}

	// map the results
	foundmorph := make(map[string]DbMorphology)
	for _, mmap := range collector {
		for w := range mmap {
			foundmorph[w] = mmap[w]
		}
	}

	return foundmorph
}

func arraytmorphologyworker(wordlist []string, uselang string, workerid int, trialnumber int, dbpool *pgxpool.Pool) map[string]DbMorphology {
	// logiflogging(fmt.Sprintf("arraytmorphologyworker %d was sent %d words", workerid, len(wordlist)), 0, 0)

	// make sure that "0" comes in last so you can watch the parallelism
	//if workerid == 0 {
	//	time.Sleep(pollinginterval)
	//	time.Sleep(pollinginterval)
	//}

	// hipparchiaDB=# CREATE TEMPORARY TABLE ttw AS SELECT words AS w FROM unnest(ARRAY['dolor', 'amor', 'lusus']) words;
	// hipparchiaDB=# SELECT observed_form, xrefs, prefixrefs, possible_dictionary_forms FROM latin_morphology
	//					WHERE EXISTS (SELECT 1 FROM ttw temptable WHERE temptable.w = latin_morphology.observed_form);

	tt := "CREATE TEMPORARY TABLE ttw_%s AS SELECT words AS w FROM unnest(ARRAY[%s]) words"
	qt := "SELECT observed_form, xrefs, prefixrefs, possible_dictionary_forms FROM %s_morphology WHERE EXISTS " +
		"(SELECT 1 FROM ttw_%s temptable WHERE temptable.w = %s_morphology.observed_form)"

	rndid := strings.Replace(uuid.New().String(), "-", "", -1)
	rndid = fmt.Sprintf("%s_%s_mw_%d", rndid, uselang, workerid)
	arr := strings.Join(wordlist, "', '")
	arr = "'" + arr + "'"
	tt = fmt.Sprintf(tt, rndid, arr)

	_, err := dbpool.Exec(context.Background(), tt)
	checkerror(err)

	foundrows, e := dbpool.Query(context.Background(), fmt.Sprintf(qt, uselang, rndid, uselang))
	// stderr=b'panic: ERROR: relation "ttw_c27067420c144eb2972034b53e77bb58_greek_mw_2" does not exist (SQLSTATE 42P01)
	// this error emerged when we moved over to goroutines
	// this only happens fairly deep into a vectorbot run
	// you never see a single error: always 2-4 dying on top of one another
	// some sort of race inside the dbpool...?
	// increasing MaxConns and/or MinConns is not the solution...
	if e != nil {
		// almost never see trial #2 & never saw #3
		trialnumber += 1
		// logiflogging(fmt.Sprintf("%s failed to create a temptable [trial #%d]", rndid, trialnumber), 0, 0)
		if trialnumber > maxtrials {
			logiflogging(fmt.Sprintf("arraytmorphologyworker worker#%d exhausted its tries to create a temptable [trial #%d]", workerid, trialnumber), 0, 0)
			logiflogging(fmt.Sprintf("your results will be INVALID: a fraction of your words were just zapped", workerid, trialnumber), 0, 0)
			return make(map[string]DbMorphology)
		} else {
			// the following makes no difference: but it maybe shows how a Begin that gave a "tx" could be used above...
			// https://gowalker.org/github.com/jackc/pgx
			//tx, err := dbpool.Begin(context.Background())
			//if err != nil {
			//	checkerror(err)
			//}
			//e = tx.Commit(context.Background())
			//if e != nil {
			//	checkerror(e)
			//}
			// logiflogging(fmt.Sprintf("TotalConns of MaxConns: %d / %d", dbpool.Stat().TotalConns(), dbpool.Stat().MaxConns()), 0, 0)
			return arraytmorphologyworker(wordlist, uselang, workerid, trialnumber, dbpool)
		}
	}

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

	tt = "DROP TABLE IF EXISTS ttw_%s"
	tt = fmt.Sprintf(tt, rndid)
	_, ee := dbpool.Exec(context.Background(), tt)
	checkerror(ee)

	// logiflogging(fmt.Sprintf("arraytmorphologyworker %d found %d items", workerid, len(foundmorph)), 0, 0)
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

func getpossiblemorph(o string, p string, pf *regexp.Regexp) MorphPossibility {
	// pf := regexp.MustCompile(`(<possibility_(\d{1,2})>)(.*?)<xref_value>(.*?)</xref_value><xref_kind>(.*?)</xref_kind>(.*?)</possibility_\d{1,2}>`)

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

	// note that in [6] you need to take the second half after the comma: "bellus" and not "bellī, bellus": s := strings.Split(x, ",")

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

func arrayfetchheadwordcounts(headwordset map[string]bool, dbpool *pgxpool.Pool) map[string]int {
	if len(headwordset) == 0 {
		return make(map[string]int)
	}

	tt := "CREATE TEMPORARY TABLE ttw_%s AS SELECT words AS w FROM unnest(ARRAY[%s]) words"
	qt := "SELECT entry_name, total_count FROM dictionary_headword_wordcounts WHERE EXISTS " +
		"(SELECT 1 FROM ttw_%s temptable WHERE temptable.w = dictionary_headword_wordcounts.entry_name)"

	rndid := strings.Replace(uuid.New().String(), "-", "", -1)

	hw := make([]string, 0, len(headwordset))
	for h := range headwordset {
		hw = append(hw, h)
	}

	arr := strings.Join(hw, "', '")
	arr = "'" + arr + "'"

	tt = fmt.Sprintf(tt, rndid, arr)
	_, err := dbpool.Exec(context.Background(), tt)
	checkerror(err)

	qt = fmt.Sprintf(qt, rndid)
	foundrows, e := dbpool.Query(context.Background(), qt)
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

	// don't kill off unfound terms
	for i := range hw {
		if _, t := returnmap[hw[i]]; t {
			continue
		} else {
			returnmap[hw[i]] = 0
		}
	}

	return returnmap
}

func parallelredisloader(workerid int, resultkey string, bags []SentenceWithLocus, redisclient *redis.Client, wg *sync.WaitGroup) {
	// logiflogging(fmt.Sprintf("parallelredisloader %d was sent %d bags", workerid, len(bags)), 0, 0)
	// make sure that "0" comes in last so you can watch the parallelism
	//if workerid == 0 {
	//	time.Sleep(pollinginterval)
	//	time.Sleep(pollinginterval)
	//}

	for i := 0; i < len(bags); i++ {
		jsonhit, err := json.Marshal(bags[i])
		checkerror(err)
		redisclient.SAdd(resultkey, jsonhit)
	}
	// don't actually need to report the key because we know it...
	// ch <- resultkey
	wg.Done()
}
