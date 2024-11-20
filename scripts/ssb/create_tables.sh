#!/bin/bash

git clone https://github.com/vadimtk/ssb-dbgen.git
cd ssb-dbgen
make
./dbgen -s 1 -T a
mkdir ../ssb/tables
mv *.tbl ../ssb/tables/
cd ..
