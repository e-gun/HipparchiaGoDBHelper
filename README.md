# HipparchiaGoDBHelper

This is a helper app that works as a plug-in to aid `HipparchiaServer`. It was originally motivated by a desire to dodge 
python's global interpreter lock and/or to help `Windows` with its painful multithreading penalties. But, in the end,
it turns out that as far as `semantic vector` work goes, this helper is 10x faster than the pure-python vector prep .
The real waiting on vectors can take place where it should as tons of linear algebra gets handled by `C`. Everyone
who uses the vector functions of `HipparchiaServer` should seriously consider installing a helper. 

The helper offloads any/all of three tasks: `websockets`, interactions with `postgresql` during searches, and `semantic vector` 
pre-processing. 

1. The `websockets` provide updates during searches. Instead of spawning a separate, immortal python thread
you launch an external program... 
1. The `grabber` lets you handle bundles of queries with multiple workers without spawning more python threads. 
   You just hand a list of work to the helper, it does the work, it hands the results back.
1. The `vector` option converts bundles of lines into bags of words that can then be vectorized.

### installation

`cli_build_archive_install.sh` will build, archive, and install into the default locations. Make sure that
`settings/helper.py` and perhaps `settings/semanticvectors.py` are properly configured in `HipparchiaServer`.


### version warning

version `1.2.0b+` **must** use morphology data from `HipparchiaBuilder 1.5.0b+`.

the columns have changed in the morphology table

if you try to use `1.2.0b+` on old data you will see the following
```
UNRECOVERABLE ERROR: PLEASE TAKE NOTE OF THE FOLLOWING PANIC MESSAGE [Hipparchia Golang Helper v.1.2.0b]
panic: ERROR: column "related_headwords" does not exist (SQLSTATE 42703)

goroutine 66 [running]:
main.checkerror(0xab3760, 0xc0011da1e0)
	/home/hipparchia/go/src/github.com/e-gun/HipparchiaGoDBHelper/hipparchiagolanghelper.go:323 +0x13f
main.morphologyworker(0xc002812e00, 0x7a70, 0x1df20, 0xa0498c, 0x5, 0x2, 0x0, 0xc0000ec8c0, 0x0)
	/home/hipparchia/go/src/github.com/e-gun/HipparchiaGoDBHelper/vectordblookups.go:159 +0x5b4
main.arraytogetrequiredmorphobjects.func1(0xc000169200, 0xc0016e8000, 0xc000302060, 0x2, 0xc002812e00, 0x7a70, 0x1df20, 0xa0498c, 0x5, 0x2, ...)
	/home/hipparchia/go/src/github.com/e-gun/HipparchiaGoDBHelper/vectordblookups.go:115 +0xd3
created by main.arraytogetrequiredmorphobjects
	/home/hipparchia/go/src/github.com/e-gun/HipparchiaGoDBHelper/vectordblookups.go:113 +0x66c

```

### more detailed overview

```
WEBSOCKETS broadcasts search information for web page updates

[a] it launches and starts listening on a port
[b] it waits to receive a websocket message: this is a search key ID (e.g., '2f81c630')
[c] it then looks inside of redis for the relevant polling data associated with that search ID
[d] it parses, packages (as JSON), and then redistributes this information back over the websocket
[e] when the poll disappears from redis, the messages stop broadcasting

```

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
for security reasons. But if you build a password into the binary, the security 
issue can be dodged.
