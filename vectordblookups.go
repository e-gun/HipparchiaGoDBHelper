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
