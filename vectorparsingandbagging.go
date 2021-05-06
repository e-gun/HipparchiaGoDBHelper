//    HipparchiaGoDBHelper: search and vector helper app and functions for HipparchiaServer
//    Copyright: E Gunderson 2016-21
//    License: GNU GENERAL PUBLIC LICENSE 3
//        (see LICENSE in the top level directory of the distribution)

package main

import (
	"github.com/jackc/pgx/v4/pgxpool"
	"regexp"
	"sort"
	"strings"
)

func stripper(item string, purge []string) string {
	// delete each in a list of items from a string
	for i := 0; i < len(purge); i++ {
		re := regexp.MustCompile(purge[i])
		item = re.ReplaceAllString(item, "")
	}
	return item
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
