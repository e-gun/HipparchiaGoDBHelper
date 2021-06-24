#!/bin/sh
# attempt to fix the import problems that gopy leaves behind
# you need gsed; "brew install gnu-sed"
A="../HipparchiaGoBinaries/module"
H="${HOME}/hipparchia_venv/HipparchiaServer/server/externalmodule/"

# toggle build style if needed
gsed -i "s/package main/package hipparchiagolangsearching/" *.go

gopy build -output=golangmodule -vm=`which python3` $GOPATH/src/github.com/e-gun/HipparchiaGoDBHelper/

# the next is no longer needed...

#gsed -i "s/import _hipparchiagolangsearching/from server.externalmodule import _hipparchiagolangsearching/" golangmodule/go.py
#gsed -i "s/import _hipparchiagolangsearching/from server.externalmodule import _hipparchiagolangsearching/" golangmodule/hipparchiagolangsearching.py
#gsed -i "s/import go/from server.externalmodule import go/" golangmodule/hipparchiagolangsearching.py

cp -rpv ./golangmodule/* ${H}
V=`grep version hipparchiagolanghelper.go | grep '= "' | cut -d '"' -f 2`
U=`uname`
mv ./golangmodule ./golangmodule-${U}-v.${V}
tar jcf ${A}/golangmodule-${U}-v.${V}.tbz ./golangmodule-${U}-v.${V}

mv ./golangmodule-${U}-v.${V} ./golangmodule-${U}-latest
tar jcf ${A}/golangmodule-${U}-latest.tbz ./golangmodule-${U}-latest

rm -rf ./golangmodule-${U}-latest
rm -rf ./golangmodule-${U}-v.${V}

echo "Latest is ${U} v.${V}" > ${A}/latest_${U}.txt

