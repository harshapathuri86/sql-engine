#!/bin/bash
if [ $# -ne 1 ]; then
    echo "Incorrect usage! Syntax: ./sql_engine.sh \"sql query\""
    exit 1
fi
python3 sql_engine.py "$1"
