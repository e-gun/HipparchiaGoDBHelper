//    HipparchiaGoDBHelper: search and vector helper app and functions for HipparchiaServer
//    Copyright: E Gunderson 2021
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

	// peek at a bag...
	//for i := 0; i < len(bags); i++ {
	//	if strings.Contains(bags[i].Sent, "ωκρ") {
	//		fmt.Println(fmt.Sprintf("%s: %s", bags[i].Loc, bags[i].Sent))
	//	}
	//}

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

	// scoremap := loopfetchheadwordcounts(allheadwords, dbpool)
	scoremap := arrayfetchheadwordcounts(allheadwords, dbpool)

	// [c] note that there are capital words in here that need lowering
	// [c1] lower the internal values first
	for i := range parsemap {
		for j := 0; j < len(parsemap[i]); j++ {
			parsemap[i][j] = strings.ToLower(parsemap[i][j])
		}
	}

	// [c2] lower the keys; how worried should we be about the collisions...
	lcparsemap := make(map[string][]string)
	for i := range parsemap {
		lcparsemap[strings.ToLower(i)] = parsemap[i]
	}

	// [d] run through the parsemap and kill off the losers

	newparsemap := make(map[string][]string)
	for i := range lcparsemap {
		var hwl WHWList
		for j := 0; j < len(lcparsemap[i]); j++ {
			var thishw WeightedHeadword
			thishw.Word = lcparsemap[i][j]
			thishw.Count = scoremap[lcparsemap[i][j]]
			hwl = append(hwl, thishw)
		}
		sort.Sort(hwl)

		newparsemap[i] = make([]string, 0, 1)
		newparsemap[i] = append(newparsemap[i], hwl[0].Word)
	}

	// [e] now you can just buildflatbagsofwords() with the new pruned parsemap

	// peek at a winner...
	//for i := range newparsemap {
	//	if strings.Contains(i, "ωκρ") {
	//		fmt.Println(fmt.Sprintf("[ZZ] %s %s", i, newparsemap[i]))
	//	}
	//}

	bags = buildflatbagsofwords(bags, newparsemap)

	return bags
}

func dropstopwords(skipper string, bagsofwords []SentenceWithLocus) []SentenceWithLocus {
	// set up the skiplist; then iterate through the bags returning new, clean bags
	s := strings.Split(skipper, " ")
	sm := make(map[string]bool)
	for i := 0; i < len(s); i++ {
		sm[s[i]] = true
	}

	for i := 0; i < len(bagsofwords); i++ {
		wl := strings.Split(bagsofwords[i].Sent, " ")
		wl = stopworddropper(sm, wl)
		bagsofwords[i].Sent = strings.Join(wl, " ")
	}

	return bagsofwords
}

func stopworddropper(stops map[string]bool, wordlist []string) []string {
	// if word is in stops, drop the word
	var returnlist []string
	for i := 0; i < len(wordlist); i++ {
		if _, t := stops[wordlist[i]]; t {
			continue
		} else {
			returnlist = append(returnlist, wordlist[i])
		}
	}
	return returnlist
}

func makesubstitutions(text string) string {
	// https://golang.org/pkg/strings/#NewReplacer
	swap := strings.NewReplacer("v", "u", "j", "i", "σ", "ϲ", "ς", "ϲ", "A.", "Aulus", "App.", "Appius",
		"C.", "Caius", "G.", "Gaius", "Cn.", "Cnaius", "Gn.", "Gnaius", "D.", "Decimus", "L.", "Lucius", "M.", "Marcus",
		"M.’", "Manius", "N.", "Numerius", "P.", "Publius", "Q.", "Quintus", "S.", "Spurius", "Sp.", "Spurius",
		"Ser.", "Servius", "Sex.", "Sextus", "T.", "Titus", "Ti", "Tiberius", "V.", "Vibius", "a.", "ante",
		"d.", "dies", "Id.", "Idibus", "Kal.", "Kalendas", "Non.", "Nonas", "prid.", "pridie", "Ian.", "Ianuarias",
		"Feb.", "Februarias", "Mart.", "Martias", "Apr.", "Aprilis", "Mai.", "Maias", "Iun.", "Iunias",
		"Quint.", "Quintilis", "Sext.", "Sextilis", "Sept.", "Septembris", "Oct.", "Octobris", "Nov.", "Novembris",
		"Dec.", "Decembris")

	return swap.Replace(text)
}
