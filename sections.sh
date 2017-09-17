#!/bin/bash

FILES="code-python/00_cdsw.py
    code-r/00_cdsw.R
    code-python/01_connect.py
    code-r/01_connect.R
    code-python/02_read.py
    code-r/02_read.R
    code-python/03_inspect.py
    code-r/03_inspect.R
    code-python/04_transform1.py
    code-python/05a_transform2.py
    code-python/05b_complex_types.py
    code-python/05c_udf.py"

for file in $FILES
do
    echo; echo "**** $file ****"; echo
    grep "# #" $file
done
