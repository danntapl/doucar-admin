#!/bin/bash

FILES="code-python/00_cdsw.py
    code-r/00_cdsw.R
    code-python/01_connect.py
    code-r/01_connect.R
    code-python/02_read.py
    code-r/02_read.R
    code-python/03_inspect.py
    code-r/03_inspect.R"

for file in $FILES
do
    echo; echo "**** $file ****"; echo
    grep "# #" $file
done
