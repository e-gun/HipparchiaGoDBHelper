//    HipparchiaGoDBHelper: search and vector helper app and functions for HipparchiaServer
//    Copyright: E Gunderson 2021
//    License: GNU GENERAL PUBLIC LICENSE 3
//        (see LICENSE in the top level directory of the distribution)

package main

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4/pgxpool"
	"regexp"
	"strings"
	"sync"
)

func fetchdblinesdirectly(dbpool *pgxpool.Pool) map[int]DbWorkline {
	thedb := cfg.VectTestDB
	thestart := cfg.VectStart
	theend := cfg.VectEnd
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

func getrequiredmorphobjects(wordlist []string) map[string]DbMorphology {
	// run arraytogetrequiredmorphobjects once for each language
	workers := cfg.WorkerCount
	pl := cfg.PGLogin

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

	lt := arraytogetrequiredmorphobjects(latinwords, "latin", workers, pl)
	gk := arraytogetrequiredmorphobjects(greekwords, "greek", workers, pl)

	mo := make(map[string]DbMorphology)
	for k, v := range gk {
		mo[k] = v
	}

	for k, v := range lt {
		mo[k] = v
	}

	return mo
}

func arraytogetrequiredmorphobjects(wordlist []string, uselang string, workercount int, pl PostgresLogin) map[string]DbMorphology {
	// NB: this goroutine version not in fact that much faster with Cicero than doing it without goroutines as one giant array
	// but the implementation pattern is likely useful for some place where it will make a difference

	// look for the upper case matches too: Ϲωκράτηϲ and not just ϲωκρατέω (!)
	var uppers []string
	for i := 0; i < len(wordlist); i++ {
		uppers = append(uppers, strings.Title(wordlist[i]))
	}

	wordlist = append(wordlist, uppers...)
	// note that we are hereby going to feed some of the workers huge lists of capitalized words that will return few hits

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
	// see the comments of Paul Hankin re. building an anonymous function

	var wg sync.WaitGroup
	var collector []map[string]DbMorphology
	outputchannels := make(chan map[string]DbMorphology, workercount)

	for i := 0; i < workercount; i++ {
		wg.Add(1)
		// "i" will be captured if sent into the function
		j := i
		go func(wordlist []string, uselang string, workerid int) {
			defer wg.Done()
			dbp := grabpgsqlconnection()
			defer dbp.Close()
			outputchannels <- morphologyworker(wordmap[j], uselang, j, 0, dbp)
		}(wordmap[i], uselang, i)
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

func morphologyworker(wordlist []string, uselang string, workerid int, trialnumber int, dbpool *pgxpool.Pool) map[string]DbMorphology {
	tt := "CREATE TEMPORARY TABLE ttw_%s AS SELECT words AS w FROM unnest(ARRAY[%s]) words"
	qt := "SELECT observed_form, xrefs, prefixrefs, related_headwords FROM %s_morphology WHERE EXISTS " +
		"(SELECT 1 FROM ttw_%s temptable WHERE temptable.w = %s_morphology.observed_form)"

	rndid := strings.Replace(uuid.New().String(), "-", "", -1)
	rndid = fmt.Sprintf("%s_%s_mw_%d", rndid, uselang, workerid)
	arr := strings.Join(wordlist, "', '")
	arr = fmt.Sprintf("'%s'", arr)
	tt = fmt.Sprintf(tt, rndid, arr)

	_, err := dbpool.Exec(context.Background(), tt)
	checkerror(err)

	foundrows, e := dbpool.Query(context.Background(), fmt.Sprintf(qt, uselang, rndid, uselang))
	// stderr=b'panic: ERROR: relation "ttw_c27067420c144eb2972034b53e77bb58_greek_mw_2" does not exist (SQLSTATE 42P01)
	// this error emerged when we moved over to goroutines
	// the fix is to give each worker its own pool rather than to share: see "dbp := grabpgsqlconnection(pl, 1, 0)" above
	// fortunately building the pools does not cost any real time
	checkerror(e)

	foundmorph := make(map[string]DbMorphology)
	defer foundrows.Close()
	count := 0
	for foundrows.Next() {
		count += 1
		var thehit DbMorphology
		err = foundrows.Scan(&thehit.Observed, &thehit.Xrefs, &thehit.PefixXrefs, &thehit.RawPossib)
		checkerror(err)
		foundmorph[thehit.Observed] = thehit
	}

	return foundmorph
}

func fetchheadwordcounts(headwordset map[string]bool, dbpool *pgxpool.Pool) map[string]int {
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
	arr = fmt.Sprintf("'%s'", arr)

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
		checkerror(err)
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
