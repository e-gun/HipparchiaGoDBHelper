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

const (
	skipheadwords = "omne νωδόϲ aut eccere quin tu ambi fue hau¹ ἠμέν eu διό πρόϲ μέχριπερ αὐτόϲ περί ἀτάρ ὄφρα ῥά¹ πω² cata apage τίϲ per1 ne³ αὐτοῦ ἕ de² chaere ἄν² ἤ¹ euoe for neque εἶἑν b ab vaha ἐπί² ὅτι Juno euhoe sum¹ au μένω sine oho sui ὡϲ ehem istic¹ τοι¹ pro² ἤ² io¹ τήιοϲ em³ ohe abusque ἆρα² alius² μέϲφι atque fu em² γοῦν proh ἔτι cis vel παι bombax τίη πηρόϲ babae en edo¹ θην δέ si ai¹ in γάρον ἐάν heia οὐδείϲ hui eho quis¹ οὐ τῇ alleluja κατά¹ οὗτοϲ καί huc tatae heus! ad non ille verus κατά γίγνομαι ἄν¹ ζεύϲ ἄνω¹ ὁτιή ἐν πῃ δέ¹ cum γάροϲ μετά ἐκάϲ ἀλλά hallelujah eheu ἐξέτι incircum mu eo¹ προϲάμβ a praeterpropter et qui¹ tat evax venio ὅϲτιϲ penes νή² εἰϲ παρά γάρ ἀμφί ἤγουν oh ἰδέ¹ trans idem ἐπάν o² ἐκ ὅϲ ito res ἀνά ἠτε tenus² τοίνυν ἐπί papae ἄτερ atat verum τιϲ ἀπέκ st heu! ah εἰ εἰμί πᾶϲ buttuti am a² hem τοιγάρ ἄλλοϲ cum¹ οὖν ambe μή vah is οὐ² ὑπέκ hic sub τε μήν¹ μά¹ καὶ¹ ἐρι² οὕτωϲ euax ὑπό ipse an¹ quam vae Q ἄνα τε¹ de prior magnus phu² προπάροιθε hehae eia oiei εἰ¹ uls aha in¹ Pollux abs πλήν δή¹ ce ὅτι¹ μέν sed ἀπό θωρακοί hei τῷ πότε ego ha! a³ prox pol ex ei² dudum διά ὁ ut ὅτι² phy fi¹ ἐπεί¹ ἐγώ ϲύ ϲύν euge ho! ὁΐ oi γε ἡμόϲ"
	skipinflected = "ita a inquit ego die nunc nos quid πάντων ἤ με θεόν δεῖ for igitur ϲύν b uers p ϲου τῷ εἰϲ ergo ἐπ ὥϲτε sua me πρό sic aut nisi rem πάλιν ἡμῶν φηϲί παρά ἔϲτι αὐτῆϲ τότε eos αὐτούϲ λέγει cum τόν quidem ἐϲτιν posse αὐτόϲ post αὐτῶν libro m hanc οὐδέ fr πρῶτον μέν res ἐϲτι αὐτῷ οὐχ non ἐϲτί modo αὐτοῦ sine ad uero fuit τοῦ ἀπό ea ὅτι parte ἔχει οὔτε ὅταν αὐτήν esse sub τοῦτο i omnes break μή ἤδη ϲοι sibi at mihi τήν in de τούτου ab omnia ὃ ἦν γάρ οὐδέν quam per α autem eius item ὡϲ sint length οὗ λόγον eum ἀντί ex uel ἐπειδή re ei quo ἐξ δραχμαί αὐτό ἄρα ἔτουϲ ἀλλ οὐκ τά ὑπέρ τάϲ μάλιϲτα etiam haec nihil οὕτω siue nobis si itaque uac erat uestig εἶπεν ἔϲτιν tantum tam nec unde qua hoc quis iii ὥϲπερ semper εἶναι e ½ is quem τῆϲ ἐγώ καθ his θεοῦ tibi ubi pro ἄν πολλά τῇ πρόϲ l ἔϲται οὕτωϲ τό ἐφ ἡμῖν οἷϲ inter idem illa n se εἰ μόνον ac ἵνα ipse erit μετά μοι δι γε enim ille an sunt esset γίνεται omnibus ne ἐπί τούτοιϲ ὁμοίωϲ παρ causa neque cr ἐάν quos ταῦτα h ante ἐϲτίν ἣν αὐτόν eo ὧν ἐπεί οἷον sed ἀλλά ii ἡ t te ταῖϲ est sit cuius καί quasi ἀεί o τούτων ἐϲ quae τούϲ minus quia tamen iam d διά primum r τιϲ νῦν illud u apud c ἐκ δ quod f quoque tr τί ipsa rei hic οἱ illi et πῶϲ φηϲίν τοίνυν s magis unknown οὖν dum text μᾶλλον λόγοϲ habet τοῖϲ qui αὐτοῖϲ suo πάντα uacat τίϲ pace ἔχειν οὐ κατά contra δύο ἔτι αἱ uet οὗτοϲ deinde id ut ὑπό τι lin ἄλλων τε tu ὁ cf δή potest ἐν eam tum μου nam θεόϲ κατ ὦ cui nomine περί atque δέ quibus ἡμᾶϲ τῶν eorum"
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
