# HipparchiaGoDBHelper

This is a merge of the soon to be defunct `HipparchiaGoGrabberModule` and `HipparchiaGoVectorHelper`.

The latter was too convergent with the former + needed to be able to grab directly for speed reasons.

```
the GRABBER is supposed to be pointedly basic

[a] it looks to redis for a pile of SQL queries that were pre-rolled
[b] it asks postgres to execute these queries
[c] it stores the results on redis
[d] it also updates the redis progress poll data relative to this search
```

```
VECTOR PREP builds bags for modeling; to do this you need to...

[a] grab db lines that are relevant to the search
[b] turn them into a unified text block
[c] do some preliminary cleanups
[d] break the text into sentences and assemble []SentenceWithLocus (NB: these are "unlemmatized bags of words")
[e] figure out all of the words used in the passage
[f] find all of the parsing info relative to these words
[g] figure out which headwords to associate with the collection of words
[h] build the lemmatized bags of words ('unlemmatized' can skip [f] and [g]...)
[i] store the bags

once you reach this point python can fetch the bags and then run "Word2Vec(bags, parameters, ...)"

```