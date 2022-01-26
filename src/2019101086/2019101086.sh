#!/bin/bash
if [ $# -ne 1 ]; then
    echo "Incorrect usage! Syntax: ./2019101086.sh \"sql query\""
    exit 1
fi
python3 2019101086.py "$1"
