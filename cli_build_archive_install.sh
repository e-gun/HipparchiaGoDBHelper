#!/bin/sh
# this presupposes that you are building the cli interface only...
# don't use this for the module: that requires the 'postmodulebuild' scripts

# toggle build style if needed
gsed -i "s/package hipparchiagolangsearching/package main/" *.go

go build
O="HipparchiaGoDBHelper"
#P="golanggrabber-cli"
P="HipparchiaGoDBHelper"
T="../HipparchiaGoBinaries/cli_prebuilt_binaries"
mv ${O} ${P}
# e.g. Hipparchia Golang Helper CLI Debugging Interface (v.0.0.1)
V=$(./${P} -v | cut -d" " -f 7 | cut -d "(" -f 2 | cut -d ")" -f 1)
U=$(uname)
H="${HOME}/hipparchia_venv/HipparchiaServer/server/golangmodule/"
cp ${P} ${H}
cp ${P} ${T}/${P}-$U-${V}
rm ${T}/${P}-${U}-${V}.bz2
bzip2 ${T}/${P}-${U}-${V}
cp ${T}/${P}-${U}-${V}.bz2 ${T}/${P}-${U}-latest.bz2

echo "Latest ${U} is ${V}" > ${T}/latest_${U}.txt