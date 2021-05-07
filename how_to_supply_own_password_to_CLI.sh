#!/bin/sh
# more an FYI on passing JSON credentials than a script as such
L='{"Host": "localhost", "Port": 5432, "User": "hippa_wr", "Pass": "MYPASSWORD", "DBName": "hipparchiaDB"}'
date && ./HipparchiaGoDBHelper -sv -l 3 -svdb lt0474 -svs 4 -sve 140000 -k "" -p ${L} && date