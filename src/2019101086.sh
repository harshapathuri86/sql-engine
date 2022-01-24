if [ $# -ne 1 ]; then
    echo "Incorrect usage! Syntax: ./2019101086.sh \"sql query\""
    exit 1
fi
python3 main_2.py "$1"
# python3 sql.py "$1"
