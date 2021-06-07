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

func arraytogetrequiredmorphobjects(wordlist []string, uselang string, workers int, dbpool *pgxpool.Pool) map[string]DbMorphology {
	// NB: this goroutine version not in fact faster with Cicero than doing it without goroutines as one giant array
	// but the implementation pattern is likely useful for some place where it will make a difference

	// look for the upper case matches too: Ϲωκράτηϲ and not just ϲωκρατέω (!)
	var uppers []string
	for i := 0; i < len(wordlist); i++ {
		uppers = append(uppers, strings.Title(wordlist[i]))
	}

	wordlist = append(wordlist, uppers...)

	totalwork := len(wordlist)
	chunksize := totalwork / workers
	leftover := totalwork % workers
	wordmap := make(map[int][]string, workers)

	if totalwork <= workers {
		wordmap[0] = wordlist
	} else {
		thestart := 0
		for i := 0; i < workers; i++ {
			wordmap[i] = wordlist[thestart : thestart+chunksize]
			thestart = thestart + chunksize
		}

		// leave no sentence behind!
		if leftover > 0 {
			wordmap[workers-1] = append(wordmap[workers-1], wordlist[totalwork-leftover-1:totalwork-1]...)
		}
	}

	// https://golangbyexample.com/return-value-goroutine-go/

	// https://stackoverflow.com/questions/46010836/using-goroutines-to-process-values-and-gather-results-into-a-slice
	// see the comments of Paul Hankin

	var wg sync.WaitGroup
	var collector []map[string]DbMorphology
	channeling := make(chan map[string]DbMorphology, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		// i will be captured if sent into the function
		j := i
		go func(wordlist []string, uselang string, workerid int, dbpool *pgxpool.Pool) {
			defer wg.Done()
			channeling <- arraytmorphologyworker(wordmap[j], uselang, j, dbpool)
		}(wordmap[i], uselang, i, dbpool)
	}

	go func() {
		wg.Wait()
		close(channeling)
	}()

	// merge the results
	for c := range channeling {
		collector = append(collector, c)
	}

	foundmorph := make(map[string]DbMorphology)
	for _, mmap := range collector {
		for w := range mmap {
			foundmorph[w] = mmap[w]
		}
	}

	return foundmorph
}

func arraytmorphologyworker(wordlist []string, uselang string, workerid int, dbpool *pgxpool.Pool) map[string]DbMorphology {

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

	// note that you can get uuid collisions when the routines go too fast; adding "uselang" and "workerid" will stop that
	rndid := strings.Replace(uuid.New().String(), "-", "", -1)
	rndid = fmt.Sprintf("%s_%s_mw_%d", rndid, uselang, workerid)
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

func parallelredisloader(resultkey string, bags []SentenceWithLocus, redisclient *redis.Client, wg *sync.WaitGroup) {
	for i := 0; i < len(bags); i++ {
		jsonhit, err := json.Marshal(bags[i])
		checkerror(err)
		redisclient.SAdd(resultkey, jsonhit)
	}
	// don't actually need to report the key because we know it...
	// ch <- resultkey
	wg.Done()
}

func loopfetchheadwordcounts(headwordset map[string]bool, dbpool *pgxpool.Pool) map[string]int {
	// the slow way: via a loop
	hw := make([]string, 0, len(headwordset))
	for h := range headwordset {
		hw = append(hw, h)
	}

	qt := "SELECT entry_name, total_count FROM dictionary_headword_wordcounts WHERE entry_name = $1"
	returnmap := make(map[string]int)
	for i := 0; i < len(hw); i++ {
		foundrows, e := dbpool.Query(context.Background(), qt, hw[i])
		checkerror(e)
		defer foundrows.Close()
		for foundrows.Next() {
			var thehit WeightedHeadword
			err := foundrows.Scan(&thehit.Word, &thehit.Count)
			if err != nil {
				fmt.Println(err)
			}
			returnmap[thehit.Word] = thehit.Count
		}
	}

	// don't kill off unfound terms

	for i := range hw {
		if _, t := returnmap[hw[i]]; t {
			continue
		} else {
			// this should be mostly proper names
			// fmt.Println(fmt.Sprintf("unfound set to 0: %s", hw[i]))
			returnmap[hw[i]] = 0
		}
	}

	return returnmap
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
