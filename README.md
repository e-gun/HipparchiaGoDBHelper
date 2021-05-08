# HipparchiaGoDBHelper

This is a merge of three now defunct projects: `HipparchiaGoGrabberModule`,  `HipparchiaGoVectorHelper`, and `HipparchiaGoWebSocketApp`.

`HipparchiaGoVectorHelper` was too convergent with `HipparchiaGoGrabberModule` + it needed to be able to grab directly 
for speed reasons. After that it seemed like one might as well just use a single binary
instead of a collection of them: so the websockets got folded in too. 

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
```
WEBSOCKETS broadcasts search information for web page updates

[a] it launches and starts listening on a port
[b] it waits to receive a websocket message: this is a search key ID (e.g., '2f81c630')
[c] it then looks inside of redis for the relevant polling data associated with that search ID
[d] it parses, packages (as JSON), and then redistributes this information back over the websocket
[e] when the poll disappears from redis, the messages stop broadcasting

```

---
## Using this as a python module


`gopy` does not automatically generate the right `import` statements in `go.py` and
`hipparchiagolangsearching.py`

You need to fix this...

This fix has been automated on macos (see `macos-module-build_install_archive.sh`).
BUT it seems like the paths are fundamentally broken for `linux` since there are bad
import calls lodged inside of the binary `.so`. Ouch.

`gopy` will fix this some day?


NB: the `websockets` can be launched as a module, but the `GIL` will render
them useless. They should be called via `subprocess.Popen()` instead. But
both the `grabber` and the `bagger` can be used either via the module or
the cli. Module invocation is in fact preferred in a multi-user environment
for security reasons. 
